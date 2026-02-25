"""
Semantic duplicate detection using embeddings with Pathway.
Detects near-duplicate invoices that may indicate fraud attempts.
"""

import pathway as pw
from typing import Optional
import numpy as np


def create_invoice_text(invoice: pw.Table) -> pw.Table:
    """
    Create a text representation of invoice for embedding.
    
    Args:
        invoice: Pathway Table with invoice data
    
    Returns:
        Pathway Table with text field
    """
    
    invoice_with_text = invoice.select(
        *pw.this,
        text=pw.cast(str, pw.this.vendor_id) + " | " +
             pw.cast(str, pw.this.description) + " | " +
             pw.cast(str, pw.this.category) + " | " +
             pw.cast(str, pw.this.amount)
    )
    
    return invoice_with_text


def compute_embeddings(
    invoices: pw.Table,
    embedding_model: str = "openai",
    model_name: str = "text-embedding-3-small"
) -> pw.Table:
    """
    Compute embeddings for invoice text using various embedding models.
    
    Args:
        invoices: Pathway Table with invoice data
        embedding_model: Type of embedding model ("openai", "sentence-transformers", "simple")
        model_name: Specific model name
    
    Returns:
        Pathway Table with embeddings
    """
    
    # Create text representation
    invoices_with_text = create_invoice_text(invoices)
    
    if embedding_model == "openai":
        # Use Pathway's OpenAI integration
        try:
            import pathway.xpacks.llm as llm
            
            embeddings = invoices_with_text.select(
                *pw.this,
                embedding=llm.embeddings.OpenAIEmbedder(
                    model=model_name
                ).embed(pw.this.text)
            )
        except Exception:
            # Fallback to simple embeddings if OpenAI is not configured
            embeddings = _compute_simple_embeddings(invoices_with_text)
    
    elif embedding_model == "sentence-transformers":
        # Use sentence-transformers model
        embeddings = _compute_sentence_transformer_embeddings(
            invoices_with_text,
            model_name
        )
    
    else:
        # Simple hash-based embeddings for testing
        embeddings = _compute_simple_embeddings(invoices_with_text)
    
    return embeddings


def _compute_simple_embeddings(invoices: pw.Table) -> pw.Table:
    """
    Compute simple hash-based embeddings for testing.
    Not suitable for production - use proper embedding models.
    
    Args:
        invoices: Pathway Table with text field
    
    Returns:
        Pathway Table with simple embeddings
    """
    
    # Use Pathway's apply to create a simple embedding
    def text_to_simple_embedding(text: str) -> list:
        """Create a simple embedding from text hash."""
        # This is a placeholder - in production use real embeddings
        hash_val = hash(text.lower())
        # Create a simple vector representation
        return [
            float((hash_val >> i) & 0xFF) / 255.0
            for i in range(0, 64, 8)
        ]
    
    embeddings = invoices.select(
        *pw.this,
        embedding=pw.apply(text_to_simple_embedding, pw.this.text)
    )
    
    return embeddings


def _compute_sentence_transformer_embeddings(
    invoices: pw.Table,
    model_name: str = "all-MiniLM-L6-v2"
) -> pw.Table:
    """
    Compute embeddings using sentence-transformers.
    
    Args:
        invoices: Pathway Table with text field
        model_name: Sentence transformer model name
    
    Returns:
        Pathway Table with embeddings
    """
    
    try:
        from sentence_transformers import SentenceTransformer
        
        model = SentenceTransformer(model_name)
        
        def embed_text(text: str) -> list:
            """Embed text using sentence transformer."""
            return model.encode(text).tolist()
        
        embeddings = invoices.select(
            *pw.this,
            embedding=pw.apply(embed_text, pw.this.text)
        )
        
        return embeddings
    
    except ImportError:
        # Fallback to simple embeddings
        return _compute_simple_embeddings(invoices)


def detect_duplicates(
    invoices: pw.Table,
    similarity_threshold: float = 0.95,
    time_window_hours: int = 24
) -> pw.Table:
    """
    Detect potential duplicate invoices using embedding similarity.
    
    Args:
        invoices: Pathway Table with embeddings
        similarity_threshold: Cosine similarity threshold for duplicates
        time_window_hours: Time window to check for duplicates
    
    Returns:
        Pathway Table with duplicate flags
    """
    
    # Compute embeddings
    invoices_with_embeddings = compute_embeddings(invoices)
    
    # Use Pathway's UDF to compute pairwise similarities
    # This is a simplified version - in production, use more efficient nearest neighbor search
    
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
    
    # Self-join to find similar invoices
    # Filter by vendor (duplicates usually target same vendor)
    similar_pairs = invoices_with_embeddings.join(
        invoices_with_embeddings,
        invoices_with_embeddings.vendor_id == invoices_with_embeddings.vendor_id
    ).select(
        invoice_id_1=invoices_with_embeddings.invoice_id,
        invoice_id_2=invoices_with_embeddings.invoice_id,
        amount_1=invoices_with_embeddings.amount,
        amount_2=invoices_with_embeddings.amount,
        timestamp_1=invoices_with_embeddings.timestamp,
        timestamp_2=invoices_with_embeddings.timestamp,
        embedding_1=invoices_with_embeddings.embedding,
        embedding_2=invoices_with_embeddings.embedding
    )
    
    # Compute similarities
    similar_pairs = similar_pairs.select(
        *pw.this,
        similarity=pw.apply(
            cosine_similarity,
            pw.this.embedding_1,
            pw.this.embedding_2
        )
    )
    
    # Filter for high similarity (excluding self-matches)
    duplicates = similar_pairs.filter(
        (pw.this.invoice_id_1 != pw.this.invoice_id_2) &
        (pw.this.similarity >= similarity_threshold)
    )
    
    return duplicates


def detect_near_duplicates_simple(
    invoices: pw.Table,
    exact_match_fields: Optional[list] = None
) -> pw.Table:
    """
    Detect exact or near-exact duplicate invoices using field matching.
    Faster alternative to embedding-based detection.
    
    Args:
        invoices: Pathway Table with invoice data
        exact_match_fields: Fields that must match exactly
    
    Returns:
        Pathway Table with duplicate information
    """
    
    if exact_match_fields is None:
        exact_match_fields = ["vendor_id", "amount", "description"]
    
    # Create a composite key for duplicate detection
    invoices_with_key = invoices.select(
        *pw.this,
        dup_key=pw.cast(str, pw.this.vendor_id) + "|" +
                pw.cast(str, pw.this.amount) + "|" +
                pw.cast(str, pw.this.description)
    )
    
    # Count occurrences of each key
    dup_counts = invoices_with_key.groupby(
        invoices_with_key.dup_key
    ).reduce(
        dup_key=invoices_with_key.dup_key,
        duplicate_count=pw.reducers.count(),
        first_invoice_id=pw.reducers.min(invoices_with_key.invoice_id),
        total_amount=pw.reducers.sum(invoices_with_key.amount)
    )
    
    # Join back to mark duplicates
    invoices_marked = invoices_with_key.join(
        dup_counts,
        invoices_with_key.dup_key == dup_counts.dup_key,
        id=invoices_with_key.id
    ).select(
        *invoices_with_key,
        is_duplicate=(dup_counts.duplicate_count > 1),
        duplicate_count=dup_counts.duplicate_count,
        duplicate_group_total=dup_counts.total_amount
    )
    
    return invoices_marked


def detect_fuzzy_duplicates(
    invoices: pw.Table,
    amount_tolerance: float = 0.01  # 1% tolerance
) -> pw.Table:
    """
    Detect fuzzy duplicates where amounts are slightly different.
    Catches attempts to evade exact duplicate detection.
    
    Args:
        invoices: Pathway Table with invoice data
        amount_tolerance: Percentage tolerance for amount matching
    
    Returns:
        Pathway Table with fuzzy duplicate flags
    """
    
    # Create normalized description and category
    normalized = invoices.select(
        *pw.this,
        desc_normalized=pw.this.description,  # In production, apply text normalization
        amount_bucket=pw.cast(int, pw.this.amount / 100) * 100  # Bucket amounts
    )
    
    # Group by vendor, normalized description, and amount bucket
    fuzzy_groups = normalized.groupby(
        normalized.vendor_id,
        normalized.desc_normalized,
        normalized.amount_bucket
    ).reduce(
        vendor_id=normalized.vendor_id,
        desc_normalized=normalized.desc_normalized,
        amount_bucket=normalized.amount_bucket,
        count_in_bucket=pw.reducers.count(),
        min_amount=pw.reducers.min(normalized.amount),
        max_amount=pw.reducers.max(normalized.amount)
    )
    
    # Calculate amount spread to detect fuzzy duplicates
    fuzzy_groups = fuzzy_groups.select(
        *pw.this,
        amount_spread_pct=((pw.this.max_amount - pw.this.min_amount) / pw.this.max_amount) * 100,
        is_fuzzy_duplicate_group=(
            (pw.this.count_in_bucket > 1) &
            (((pw.this.max_amount - pw.this.min_amount) / pw.this.max_amount) <= amount_tolerance)
        )
    )
    
    return fuzzy_groups


if __name__ == "__main__":
    # Example usage
    from invoice_stream import generate_invoice_stream
    
    print("Testing duplicate detection...")
    
    # Generate sample invoices
    invoices = generate_invoice_stream(num_invoices=50)
    
    # Detect simple duplicates
    duplicates = detect_near_duplicates_simple(invoices)
    
    # Display results
    print("\nDuplicate Detection Results:")
    pw.debug.compute_and_print(duplicates.filter(pw.this.is_duplicate))
