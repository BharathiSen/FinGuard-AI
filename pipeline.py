"""
Main Pathway streaming pipeline for real-time fraud detection.
Orchestrates all components: ingestion, vendor tracking, duplicate detection,
risk scoring, and explanation generation.

This is a PURE STREAMING pipeline - all updates happen automatically as new invoices arrive.
"""

import pathway as pw
from typing import Optional
import os

# Import pipeline components
from invoice_stream import generate_invoice_stream
from vendor_state import (
    track_vendor_state,
    enrich_with_vendor_context,
    detect_vendor_anomalies
)
from duplicate_detector import detect_near_duplicates_simple
from semantic_duplicates import (
    detect_semantic_duplicates,
    enrich_with_semantic_duplicate_info
)
from risk_engine import (
    compute_composite_risk_score,
    compute_realtime_risk_score,
    filter_high_risk_invoices,
    create_risk_alerts,
    apply_autonomous_decision
)
from llm_explainer import (
    generate_rule_based_explanation,
    generate_llm_explanation,
    generate_explanations_for_invoices
)


class FraudDetectionPipeline:
    """
    Real-time fraud detection pipeline using Pathway streaming.
    
    This pipeline automatically updates when new invoices arrive:
    1. Ingests invoice stream (simulated or real)
    2. Tracks vendor behavior incrementally
    3. Detects duplicates using streaming joins
    4. Computes risk scores dynamically
    5. Generates explanations in real-time
    6. Outputs alerts continuously
    
    All processing is event-driven - no batch computation.
    """
    
    def __init__(
        self,
        use_llm: bool = False,
        llm_model: str = "gpt-4o-mini",
        use_semantic_duplicates: bool = False,
        semantic_threshold: float = 0.85,
        num_invoices: Optional[int] = None,
        use_realtime_scoring: bool = True
    ):
        """
        Initialize the fraud detection pipeline.
        
        Args:
            use_llm: Whether to use LLM for explanations (requires API key)
            llm_model: LLM model to use
            use_semantic_duplicates: Use semantic (embedding-based) duplicate detection
            semantic_threshold: Similarity threshold for semantic duplicates (default: 0.85)
            num_invoices: Number of invoices to generate (None = infinite stream)
            use_realtime_scoring: Use focused real-time risk scoring (rule-based, transparent)
        """
        self.use_llm = use_llm
        self.llm_model = llm_model
        self.use_semantic_duplicates = use_semantic_duplicates
        self.semantic_threshold = semantic_threshold
        self.num_invoices = num_invoices
        self.use_realtime_scoring = use_realtime_scoring
        
        # Pipeline streaming tables (all auto-updating)
        self.invoices = None
        self.vendor_stats = None
        self.enriched_invoices = None
        self.duplicates = None
        self.risk_scores = None
        self.realtime_risk = None  # 📊 Focused real-time risk scores
        self.decisions = None  # 🔥 Autonomous decisions
        self.high_risk_alerts = None
        self.explanations = None
        self.explained_invoices = None  # 📝 All invoices with professional explanations
    
    def setup_input_stream(self) -> pw.Table:
        """Set up the real-time invoice stream."""
        
        print("🔴 Starting live invoice stream...")
        print("⏱️  New invoice every 5 seconds...")
        invoices = generate_invoice_stream(
            num_invoices=self.num_invoices,
            interval_ms=5000  # 5 seconds between invoices
        )
        
        self.invoices = invoices
        return invoices
    
    def compute_vendor_intelligence(self):
        """Compute vendor statistics and patterns."""
        
        print("Computing vendor intelligence...")
        
        # Track vendor statistics incrementally
        self.vendor_stats = track_vendor_state(self.invoices)
        
        # Enrich invoices with vendor context
        self.enriched_invoices = enrich_with_vendor_context(
            self.invoices,
            self.vendor_stats
        )
        
        # Detect vendor anomalies
        self.anomalies = detect_vendor_anomalies(
            self.invoices,
            self.vendor_stats
        )
    
    def detect_fraud_patterns(self):
        """Detect various fraud patterns using streaming operations."""
        
        print("🔍 Detecting fraud patterns...")
        
        if self.use_semantic_duplicates:
            print("   🧠 Using semantic duplicate detection (embeddings)...")
            try:
                # Detect semantic duplicates using embeddings
                self.duplicates = detect_semantic_duplicates(
                    self.invoices,
                    similarity_threshold=self.semantic_threshold
                )
            except ImportError:
                print("   ⚠️  sentence-transformers not installed, falling back to simple detection")
                self.duplicates = detect_near_duplicates_simple(self.invoices)
            except Exception as e:
                print(f"   ⚠️  Semantic detection failed ({e}), using simple detection")
                self.duplicates = detect_near_duplicates_simple(self.invoices)
        else:
            print("   ⚡ Using fast exact-match duplicate detection...")
            # Detect duplicates using streaming joins (faster)
            self.duplicates = detect_near_duplicates_simple(self.invoices)
    
    def compute_risk_assessment(self):
        """Compute comprehensive risk scores."""
        
        print("💯 Computing risk scores...")
        
        if self.use_realtime_scoring:
            print("   📊 Using real-time focused risk scoring (transparent rules)...")
            # Compute focused real-time risk scores (clean, rule-based)
            self.realtime_risk = compute_realtime_risk_score(self.enriched_invoices)
            # Use realtime risk for decisions
            self.risk_scores = self.realtime_risk
        else:
            print("   🔬 Using composite risk scoring (multi-factor)...")
            # Compute composite risk scores (complex)
            self.risk_scores = compute_composite_risk_score(
                self.enriched_invoices,
                duplicate_info=self.duplicates
            )
        
        print("🔥 Applying autonomous decision engine...")
        
        # Apply autonomous decision logic (Auto Approve/Review/Reject)
        self.decisions = apply_autonomous_decision(self.risk_scores)
        
        # Filter high-risk invoices
        self.high_risk_alerts = filter_high_risk_invoices(
            self.decisions,
            threshold=0.6
        )
    
    def generate_explanations(self):
        """Generate human-readable explanations in real-time."""
        
        print("💡 Generating explanations...")
        
        # Generate professional audit-ready explanations for ALL invoices
        print("   📝 Generating professional explanations for all invoices...")
        try:
            self.explained_invoices = generate_explanations_for_invoices(
                self.risk_scores
            )
        except Exception as e:
            print(f"   ⚠️  Professional explanation generation failed: {e}")
            self.explained_invoices = self.risk_scores
        
        # Generate detailed explanations for high-risk invoices only
        print("   🚨 Generating detailed explanations for high-risk alerts...")
        if self.use_llm:
            try:
                self.explanations = generate_llm_explanation(
                    self.high_risk_alerts,
                    model=self.llm_model
                )
            except Exception as e:
                print(f"   ⚠️  LLM failed, using rule-based: {e}")
                self.explanations = generate_rule_based_explanation(
                    self.high_risk_alerts
                )
        else:
            self.explanations = generate_rule_based_explanation(
                self.high_risk_alerts
            )
    
    def setup_outputs(self):
        """Set up real-time output streams."""
        
        print("📤 Setting up real-time outputs...")
        
        # Create output directory
        os.makedirs("output", exist_ok=True)
        
        # Write streaming output - auto-updates as new invoices arrive
        
        # High-risk alerts only
        pw.io.jsonlines.write(
            self.high_risk_alerts,
            "output/high_risk_alerts.jsonl"
        )
        
        # Detailed explanations for high-risk invoices
        pw.io.jsonlines.write(
            self.explanations,
            "output/explanations.jsonl"
        )
        
        # Vendor statistics (incremental aggregations)
        pw.io.jsonlines.write(
            self.vendor_stats,
            "output/vendor_stats.jsonl"
        )
        
        # 🔥 Autonomous decisions (all invoices)
        pw.io.jsonlines.write(
            self.decisions,
            "output/autonomous_decisions.jsonl"
        )
        
        # 📝 All invoices with professional explanations
        if self.explained_invoices is not None:
            pw.io.jsonlines.write(
                self.explained_invoices,
                "output/all_invoices_explained.jsonl"
            )
        
        # 📊 Real-time risk scores (if using focused scoring)
        if self.use_realtime_scoring and self.realtime_risk is not None:
            pw.io.jsonlines.write(
                self.realtime_risk,
                "output/realtime_risk_scores.jsonl"
            )
    
    def run(self):
        """
        Execute the complete streaming pipeline.
        
        This sets up the Pathway computation graph and starts processing.
        All tables auto-update when new invoices arrive - NO recomputation!
        """
        
        print("\n" + "=" * 70)
        print("🛡️  FinGuard AI - Real-Time Fraud Detection")
        print("=" * 70)
        print("⚡ Powered by Pathway Streaming Engine")
        print("=" * 70 + "\n")
        
        # Set up streaming computation graph
        print("🔧 Configuring streaming pipeline...\n")
        
        self.setup_input_stream()
        print("   ✓ Invoice stream configured")
        
        self.compute_vendor_intelligence()
        print("   ✓ Vendor intelligence layer configured")
        
        self.detect_fraud_patterns()
        print("   ✓ Fraud pattern detection configured")
        
        self.compute_risk_assessment()
        print("   ✓ Risk scoring engine configured")
        
        self.generate_explanations()
        print("   ✓ Explanation generator configured")
        
        self.setup_outputs()
        print("   ✓ Output streams configured")
        
        print("\n" + "=" * 70)
        print("✅ Pipeline ready! All tables will auto-update on new data.")
        print("=" * 70)
        print("\n🚀 Starting real-time processing...\n")
        print("📊 Monitor alerts: tail -f output/high_risk_alerts.jsonl")
        print("📈 Dashboard: streamlit run streamlit_app.py\n")
        print("Press Ctrl+C to stop\n")
        
        # Start the Pathway streaming engine
        pw.run()


def create_monitoring_tables(pipeline: FraudDetectionPipeline):
    """
    Create additional streaming tables for dashboard monitoring.
    All tables auto-update in real-time.
    
    Args:
        pipeline: Configured FraudDetectionPipeline
    
    Returns:
        Dictionary of monitoring tables
    """
    
    # Summary statistics - auto-updating
    alert_summary = pipeline.high_risk_alerts.groupby(
        pipeline.high_risk_alerts.risk_level
    ).reduce(
        risk_level=pipeline.high_risk_alerts.risk_level,
        count=pw.reducers.count(),
        avg_score=pw.reducers.avg(pipeline.high_risk_alerts.risk_score)
    )
    
    # Vendor risk summary - incremental
    vendor_risk_summary = pipeline.risk_scores.groupby(
        pipeline.risk_scores.vendor_id
    ).reduce(
        vendor_id=pipeline.risk_scores.vendor_id,
        total_invoices=pw.reducers.count(),
        high_risk_count=pw.reducers.count(pw.if_else(
            pipeline.risk_scores.risk_level == "HIGH",
            1,
            None
        )),
        avg_risk_score=pw.reducers.avg(pipeline.risk_scores.risk_score),
        total_amount=pw.reducers.sum(pipeline.risk_scores.amount)
    )
    
    return {
        "alert_summary": alert_summary,
        "vendor_risk_summary": vendor_risk_summary,
        "vendor_stats": pipeline.vendor_stats,
        "all_risk_scores": pipeline.risk_scores,
        "high_risk_alerts": pipeline.high_risk_alerts
    }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="🛡️ FinGuard AI - Real-Time Fraud Detection (Pathway Streaming)"
    )
    parser.add_argument(
        "--use-llm",
        action="store_true",
        help="Enable LLM-based explanations (requires OPENAI_API_KEY)"
    )
    parser.add_argument(
        "--use-semantic-duplicates",
        action="store_true",
        help="Use semantic (embedding-based) duplicate detection (requires sentence-transformers)"
    )
    parser.add_argument(
        "--semantic-threshold",
        type=float,
        default=0.85,
        help="Similarity threshold for semantic duplicates (default: 0.85)"
    )
    parser.add_argument(
        "--num-invoices",
        type=int,
        default=None,
        help="Number of invoices to generate (default: infinite stream)"
    )
    parser.add_argument(
        "--use-composite-scoring",
        action="store_true",
        help="Use composite risk scoring instead of focused real-time scoring"
    )
    
    args = parser.parse_args()
    
    try:
        # Create and run streaming pipeline
        pipeline = FraudDetectionPipeline(
            use_llm=args.use_llm,
            use_semantic_duplicates=args.use_semantic_duplicates,
            semantic_threshold=args.semantic_threshold,
            num_invoices=args.num_invoices,
            use_realtime_scoring=not args.use_composite_scoring
        )
        
        pipeline.run()
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Pipeline stopped by user.")
    except Exception as e:
        print(f"\n\n❌ Pipeline error: {e}")
        raise

