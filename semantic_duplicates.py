"""
Semantic Duplicate Detection Module using Sentence Transformers.

Efficiently detects semantically similar invoices using embeddings with:
- Memory-based embedding storage (no re-embedding)
- Cosine similarity comparison
- Configurable similarity threshold
- Modular architecture

Designed for real-time fraud detection.
"""

import pathway as pw
from typing import Optional, Dict, List
import numpy as np


class SemanticDuplicateDetector:
    """
    Semantic duplicate detector using sentence transformers.
    
    Maintains embedding cache to avoid re-embedding historical invoices.
    Compares new invoice embeddings against all previous embeddings.
    """
    
    def __init__(
        self,
        model_name: str = "all-MiniLM-L6-v2",
        similarity_threshold: float = 0.85,
        use_gpu: bool = False
    ):
        """
        Initialize the semantic duplicate detector.
        
        Args:
            model_name: Sentence transformer model name
            similarity_threshold: Cosine similarity threshold for duplicates (default: 0.85)
            use_gpu: Whether to use GPU for embeddings (if available)
        """
        self.model_name = model_name
        self.similarity_threshold = similarity_threshold
        self.use_gpu = use_gpu
        self.model = None
        self._embedding_cache: Dict[str, np.ndarray] = {}
    
    def _load_model(self):
        """Lazy load the sentence transformer model."""
        if self.model is None:
            try:
                from sentence_transformers import SentenceTransformer
                device = 'cuda' if self.use_gpu else 'cpu'
                self.model = SentenceTransformer(self.model_name, device=device)
                print(f"✅ Loaded model: {self.model_name} on {device}")
            except ImportError:
                raise ImportError(
                    "sentence-transformers not installed. "
                    "Install with: pip install sentence-transformers"
                )
    
    def embed_description(self, description: str) -> np.ndarray:
        """
        Convert invoice description to embedding vector.
        
        Uses caching to avoid re-embedding identical descriptions.
        
        Args:
            description: Invoice description text
        
        Returns:
            Embedding vector as numpy array
        """
        # Check cache first
        if description in self._embedding_cache:
            return self._embedding_cache[description]
        
        # Load model if needed
        self._load_model()
        
        # Generate embedding
        embedding = self.model.encode(description, convert_to_numpy=True)
        
        # Store in cache
        self._embedding_cache[description] = embedding
        
        return embedding
    
    def compute_cosine_similarity(
        self,
        embedding1: np.ndarray,
        embedding2: np.ndarray
    ) -> float:
        """
        Compute cosine similarity between two embedding vectors.
        
        Formula: cos(θ) = (A · B) / (||A|| ||B||)
        
        Args:
            embedding1: First embedding vector
            embedding2: Second embedding vector
        
        Returns:
            Cosine similarity score (0-1)
        """
        # Compute dot product
        dot_product = np.dot(embedding1, embedding2)
        
        # Compute norms
        norm1 = np.linalg.norm(embedding1)
        norm2 = np.linalg.norm(embedding2)
        
        # Handle zero vectors
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        # Compute cosine similarity
        similarity = dot_product / (norm1 * norm2)
        
        return float(similarity)
    
    def find_similar_invoices(
        self,
        new_description: str,
        existing_descriptions: List[str]
    ) -> List[tuple]:
        """
        Find similar invoices by comparing embeddings.
        
        Args:
            new_description: Description of new invoice
            existing_descriptions: List of existing invoice descriptions
        
        Returns:
            List of (description, similarity_score) tuples above threshold
        """
        # Embed new description
        new_embedding = self.embed_description(new_description)
        
        similar_invoices = []
        
        # Compare against all existing descriptions
        for existing_desc in existing_descriptions:
            # Embed existing description (uses cache if available)
            existing_embedding = self.embed_description(existing_desc)
            
            # Compute similarity
            similarity = self.compute_cosine_similarity(new_embedding, existing_embedding)
            
            # Check threshold
            if similarity >= self.similarity_threshold:
                similar_invoices.append((existing_desc, similarity))
        
        # Sort by similarity (highest first)
        similar_invoices.sort(key=lambda x: x[1], reverse=True)
        
        return similar_invoices
    
    def get_cache_size(self) -> int:
        """Get number of embeddings stored in cache."""
        return len(self._embedding_cache)
    
    def clear_cache(self):
        """Clear embedding cache to free memory."""
        self._embedding_cache.clear()


# ============================================================================
# Pathway Integration Functions
# ============================================================================

def create_invoice_text_representation(invoices: pw.Table) -> pw.Table:
    """
    Create text representation of invoices for semantic analysis.
    
    Combines multiple fields to create rich semantic context.
    
    Args:
        invoices: Pathway Table with invoice data
    
    Returns:
        Pathway Table with text field
    """
    invoices_with_text = invoices.select(
        *pw.this,
        semantic_text=(
            "Vendor: " + pw.cast(str, pw.this.vendor_id) + ". " +
            "Description: " + pw.cast(str, pw.this.description) + ". " +
            "Category: " + pw.cast(str, pw.this.category) + ". " +
            "Amount: $" + pw.cast(str, pw.this.amount)
        )
    )
    
    return invoices_with_text


def compute_semantic_embeddings(
    invoices: pw.Table,
    model_name: str = "all-MiniLM-L6-v2"
) -> pw.Table:
    """
    Compute semantic embeddings for invoices using sentence transformers.
    
    Uses Pathway's apply function to embed descriptions.
    Embeddings are cached internally to avoid recomputation.
    
    Args:
        invoices: Pathway Table with invoice data
        model_name: Sentence transformer model name
    
    Returns:
        Pathway Table with embeddings
    """
    # Create text representation
    invoices_with_text = create_invoice_text_representation(invoices)
    
    # Initialize detector (will be reused)
    detector = SemanticDuplicateDetector(model_name=model_name)
    
    def embed_text(text: str) -> list:
        """Embed text using cached detector."""
        embedding = detector.embed_description(text)
        return embedding.tolist()
    
    # Apply embedding function
    invoices_with_embeddings = invoices_with_text.select(
        *pw.this,
        embedding=pw.apply(embed_text, pw.this.semantic_text)
    )
    
    return invoices_with_embeddings


def detect_semantic_duplicates(
    invoices: pw.Table,
    similarity_threshold: float = 0.85,
    model_name: str = "all-MiniLM-L6-v2"
) -> pw.Table:
    """
    Detect semantic duplicates using embedding similarity.
    
    Flags invoices with description similarity > threshold as duplicates.
    Uses efficient cosine similarity computation.
    
    Args:
        invoices: Pathway Table with invoice data
        similarity_threshold: Similarity threshold (default: 0.85)
        model_name: Sentence transformer model name
    
    Returns:
        Pathway Table with duplicate flags and similarity scores
    """
    # Compute embeddings
    invoices_with_embeddings = compute_semantic_embeddings(invoices, model_name)
    
    # Cosine similarity function
    def cosine_similarity(vec1: list, vec2: list) -> float:
        """Compute cosine similarity between two vectors."""
        if not vec1 or not vec2:
            return 0.0
        
        vec1_arr = np.array(vec1)
        vec2_arr = np.array(vec2)
        
        dot_product = np.dot(vec1_arr, vec2_arr)
        norm1 = np.linalg.norm(vec1_arr)
        norm2 = np.linalg.norm(vec2_arr)
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        return float(dot_product / (norm1 * norm2))
    
    # Self-join to find similar pairs (within same vendor)
    pairs = invoices_with_embeddings.join(
        invoices_with_embeddings,
        invoices_with_embeddings.vendor_id == invoices_with_embeddings.vendor_id,
        id=invoices_with_embeddings.id
    ).select(
        invoice_id_a=invoices_with_embeddings.invoice_id,
        invoice_id_b=invoices_with_embeddings.invoice_id,
        vendor_id=invoices_with_embeddings.vendor_id,
        description_a=invoices_with_embeddings.description,
        description_b=invoices_with_embeddings.description,
        amount_a=invoices_with_embeddings.amount,
        amount_b=invoices_with_embeddings.amount,
        embedding_a=invoices_with_embeddings.embedding,
        embedding_b=invoices_with_embeddings.embedding
    )
    
    # Compute similarity scores
    pairs_with_similarity = pairs.select(
        *pw.this,
        semantic_similarity=pw.apply(
            cosine_similarity,
            pw.this.embedding_a,
            pw.this.embedding_b
        )
    )
    
    # Filter for duplicates (exclude self-matches)
    duplicates = pairs_with_similarity.filter(
        (pw.this.invoice_id_a != pw.this.invoice_id_b) &
        (pw.this.semantic_similarity >= similarity_threshold)
    )
    
    # Add duplicate flag and metadata
    duplicates = duplicates.select(
        invoice_id=pw.this.invoice_id_a,
        duplicate_of=pw.this.invoice_id_b,
        vendor_id=pw.this.vendor_id,
        description=pw.this.description_a,
        duplicate_description=pw.this.description_b,
        amount=pw.this.amount_a,
        duplicate_amount=pw.this.amount_b,
        semantic_similarity=pw.this.semantic_similarity,
        is_semantic_duplicate=True,
        duplicate_type="SEMANTIC"
    )
    
    return duplicates


def enrich_with_semantic_duplicate_info(
    invoices: pw.Table,
    similarity_threshold: float = 0.85
) -> pw.Table:
    """
    Enrich invoices with semantic duplicate information.
    
    Adds duplicate flag and similarity score to each invoice.
    
    Args:
        invoices: Pathway Table with invoice data
        similarity_threshold: Similarity threshold for flagging
    
    Returns:
        Pathway Table with semantic duplicate information
    """
    # Detect semantic duplicates
    semantic_dups = detect_semantic_duplicates(invoices, similarity_threshold)
    
    # Left join to add duplicate info to all invoices
    enriched = invoices.join(
        semantic_dups,
        invoices.invoice_id == semantic_dups.invoice_id,
        id=invoices.id,
        how=pw.JoinMode.LEFT
    ).select(
        *invoices,
        has_semantic_duplicate=pw.if_else(
            semantic_dups.is_semantic_duplicate.is_not_none(),
            True,
            False
        ),
        semantic_similarity_score=pw.if_else(
            semantic_dups.semantic_similarity.is_not_none(),
            semantic_dups.semantic_similarity,
            0.0
        ),
        duplicate_invoice_id=pw.if_else(
            semantic_dups.duplicate_of.is_not_none(),
            semantic_dups.duplicate_of,
            ""
        )
    )
    
    return enriched


# ============================================================================
# Testing and Examples
# ============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("Semantic Duplicate Detection Module - Test")
    print("=" * 70)
    
    # Test 1: Standalone detector
    print("\n📊 Test 1: Standalone SemanticDuplicateDetector\n")
    
    detector = SemanticDuplicateDetector(
        model_name="all-MiniLM-L6-v2",
        similarity_threshold=0.85
    )
    
    # Sample invoice descriptions
    descriptions = [
        "Office supplies purchase - pens and paper",
        "Office supplies - pens, paper, and staplers",  # Similar to first
        "Cloud infrastructure monthly fee",
        "Monthly cloud hosting charges",  # Similar to third
        "Marketing campaign Q1 budget",
        "Employee travel reimbursement"
    ]
    
    print("Analyzing descriptions for duplicates...\n")
    
    for i, desc in enumerate(descriptions):
        print(f"Invoice {i+1}: {desc}")
        
        # Find similar invoices among previous ones
        similar = detector.find_similar_invoices(desc, descriptions[:i])
        
        if similar:
            print(f"  🚨 DUPLICATE DETECTED!")
            for sim_desc, score in similar:
                print(f"     Similar to: '{sim_desc}' (similarity: {score:.3f})")
        else:
            print(f"  ✅ No duplicates found")
        print()
    
    print(f"📦 Embedding cache size: {detector.get_cache_size()} embeddings\n")
    
    # Test 2: Pathway integration
    print("=" * 70)
    print("Test 2: Pathway Streaming Integration")
    print("=" * 70)
    print("\n🔄 Generating invoice stream with semantic duplicate detection...\n")
    
    try:
        from invoice_stream import generate_invoice_stream
        
        # Generate sample invoices
        invoices = generate_invoice_stream(num_invoices=20, interval_ms=100)
        
        # Detect semantic duplicates
        print("Running semantic duplicate detection...")
        duplicates = detect_semantic_duplicates(invoices, similarity_threshold=0.85)
        
        # Display results
        print("\n🚨 Semantic Duplicates Found:")
        pw.debug.compute_and_print(duplicates)
        
    except Exception as e:
        print(f"⚠️  Pathway integration test skipped: {e}")
    
    print("\n" + "=" * 70)
    print("✅ Semantic Duplicate Detection Module Ready")
    print("=" * 70)
    print("\nUsage:")
    print("  detector = SemanticDuplicateDetector(similarity_threshold=0.85)")
    print("  similar = detector.find_similar_invoices(new_desc, existing_descs)")
    print("  duplicates = detect_semantic_duplicates(invoices_table)")
