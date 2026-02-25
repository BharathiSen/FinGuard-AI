# FinGuard AI - Real-Time Financial Fraud Detection

**A pure Pathway streaming system for real-time financial fraud detection with Autonomous Decision Engine**

Built for **Hack For Green Bharat - Pathway Track**

## 🎯 Overview

FinGuard AI is a **100% streaming-based** fraud detection system that:
- ✅ Uses **Pathway** as the core real-time streaming engine
- ✅ **🔥 Autonomous Decision Engine** - Auto approve/review/reject invoices
- ✅ Automatically updates when new invoices arrive (NO reprocessing)
- ✅ Performs **incremental aggregations** for vendor tracking
- ✅ Detects fraud patterns in real-time using streaming joins
- ✅ Provides human-readable explanations with optional LLM integration
- ✅ Powers a live Streamlit dashboard

**NO BATCH PROCESSING. NO PANDAS IN CORE PIPELINE. PURE EVENT-DRIVEN ARCHITECTURE.**

## 🔥 Autonomous Decision Engine

**AI-powered invoice processing automation:**

| Risk Score | Decision | Action |
|------------|----------|--------|
| **< 30%** | ✅ **AUTO_APPROVE** | Fast-track payment, no review needed |
| **30-70%** | ⚠️ **REVIEW_REQUIRED** | Queue for manual verification |
| **> 70%** | 🚫 **AUTO_REJECT** | Block payment, trigger alert |

**Benefits:**
- 🚀 **Instant Processing**: Low-risk invoices approved in real-time
- 🎯 **Focused Reviews**: Analysts only see questionable cases
- 🛡️ **Automatic Protection**: High-risk invoices blocked immediately
- 📊 **Full Transparency**: Every decision includes confidence score and reasoning

## 🏗️ Architecture

```
invoice_stream.py       → Real-time invoice generation (Pathway streaming)
vendor_state.py         → Incremental vendor statistics (auto-updating)
duplicate_detector.py   → Streaming duplicate detection (exact/fuzzy)
semantic_duplicates.py  → 🧠 Semantic duplicate detection (embeddings)
risk_engine.py          → Dynamic risk scoring + 🔥 Autonomous decisions
llm_explainer.py        → Real-time explanation generation
pipeline.py             → Main streaming orchestrator
streamlit_app.py        → Live monitoring dashboard
```

### Data Flow (Event-Driven)

```
┌─────────────────┐
│ Invoice Stream  │──► Pathway Table (auto-updating)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Vendor Stats    │──► Incremental Aggregations
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Duplicate Check │──► Streaming Joins
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Risk Scoring   │──► Dynamic Computation
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 🔥 Autonomous   │──► Auto Approve/Review/Reject
│    Decisions    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Explanations   │──► Real-time Generation
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Alerts + Output │──► JSONL Files + Dashboard
└─────────────────┘
```

**All transformations happen automatically as new invoices arrive!**

## 🚀 Quick Start

### 1. Installation

```bash
# Clone the repository
git clone https://github.com/BharathiSen/FinGuard-AI.git
cd FinGuard-AI

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Run the Streaming Pipeline

**Basic Mode:**
```bash
python pipeline.py
```

**With Semantic Duplicate Detection:**
```bash
# Uses sentence transformers for embedding-based duplicate detection
python pipeline.py --use-semantic-duplicates
```

**With LLM Explanations:**
```bash
# Set your OpenAI API key
export OPENAI_API_KEY="your-key-here"  # Windows: set OPENAI_API_KEY=your-key-here

python pipeline.py --use-llm
```

**Advanced Mode (All Features):**
```bash
# Semantic duplicates + LLM explanations + custom threshold
python pipeline.py --use-semantic-duplicates --semantic-threshold 0.85 --use-llm
```

**Finite Stream (for testing):**
```bash
python pipeline.py --num-invoices 100
```

### 3. Launch Real-Time Dashboard

In a separate terminal:
```bash
streamlit run streamlit_app.py
```

Visit `http://localhost:8501` to see live alerts updating automatically!

## 📊 Key Features

### Live Invoice Stream (Event-Driven)

**Simulated live invoice generation:**
- ⏱️ **New invoice every 5 seconds** (configurable interval)
- 🔄 **Automatic downstream updates** - each invoice triggers the entire pipeline
- 📋 **Complete invoice schema:**
  ```
  invoice_id      - Unique identifier
  vendor_id       - Vendor reference
  amount          - Invoice amount
  tax             - Calculated tax
  bank_account    - Vendor bank account
  timestamp       - ISO timestamp
  description     - Invoice description
  category        - Expense category
  payment_method  - Payment type
  ```
- 🚫 **No static CSV or pandas** - pure Pathway streaming
- 🎯 **Fraud pattern injection** - 15% of invoices contain suspicious patterns

**Implementation:**
```python
# Uses Pathway's demo streaming connector
invoices = pw.demo.generate_custom_stream(
    generate_invoices,  # Lazy generator function
    schema=InvoiceSchema,
    input_rate=0.2  # 5 seconds between invoices
)
```

### Pure Streaming Architecture
- ✅ **Event-driven:** All processing triggered by new data
- ✅ **Incremental:** No full dataset recomputation
- ✅ **Stateful:** Pathway maintains vendor statistics automatically
- ✅ **Real-time:** Sub-second latency for fraud detection

### Fraud Detection Capabilities

**🔄 Stateful Vendor Tracking:**
- **Rolling average invoice amount** - updates incrementally on each new invoice
- **Deviation percentage detection** - compares current amount vs vendor's rolling average
  - `deviation % = ((amount - avg) / avg) * 100`
  - Flags invoices with >50% deviation automatically
- **Bank account change detection** - tracks last known bank account per vendor
  - Detects when vendor changes bank account (HIGH RISK for payment fraud)
  - Uses `pw.reducers.latest()` for stateful tracking
- **Incremental aggregations** - count, sum, avg, min, max, stddev
  - All statistics auto-update without full dataset recomputation
  - Uses Pathway's `groupby().reduce()` pattern
- **Streaming joins** - enriches each invoice with vendor context in real-time

**Technical Implementation:**
```python
# Stateful vendor statistics (auto-updating)
vendor_stats = invoices.groupby(vendor_id).reduce(
    avg_amount=pw.reducers.avg(amount),          # Rolling average
    last_bank_account=pw.reducers.latest(bank_account)  # State tracking
)

# Deviation detection (streaming join)
enriched = invoices.join(vendor_stats).select(
    deviation_percentage=((amount - avg_amount) / avg_amount) * 100,
    bank_account_changed=(bank_account != last_bank_account)
)
```

**Vendor Behavior Analysis:**
- Z-score based anomaly detection (statistical outliers)
- New vendor flagging (first 3-5 invoices)
- Historical max exceeded detection
- Category usage pattern tracking

**Duplicate Detection:**

*Fast Mode (Default):*
- Exact duplicate matching using streaming joins
- Fuzzy duplicate detection (similar amounts within tolerance)
- Field-based comparison (vendor + amount + description)

*🧠 Semantic Mode (Optional):*
- **Embedding-based duplicate detection** using sentence transformers
- Detects semantically similar invoices even with different wording
- Example: "Monthly cloud hosting" ≈ "Cloud infrastructure fee" (similarity: 0.87)
- **Memory-efficient caching** - embeddings stored to avoid recomputation
- **Cosine similarity comparison** - flags duplicates if similarity > 0.85
- **Modular architecture** - standalone module usable outside pipeline

```python
# Semantic duplicate detection
from semantic_duplicates import SemanticDuplicateDetector

detector = SemanticDuplicateDetector(similarity_threshold=0.85)

# New invoice arrives
similar = detector.find_similar_invoices(
    new_description="Monthly cloud fee",
    existing_descriptions=["Cloud hosting charges", "Office supplies"]
)
# Returns: [("Cloud hosting charges", 0.89)] - DUPLICATE DETECTED

# Embeddings cached - no re-embedding of historical invoices
```

**How it works:**
1. Convert description → embedding (384-dim vector)
2. Store embedding in memory cache (key: description)
3. Compare new embedding vs all cached embeddings
4. Compute cosine similarity: `cos(θ) = (A·B) / (||A|| ||B||)`
5. Flag as duplicate if similarity ≥ threshold

**Risk Scoring:**
- Multi-factor scoring: amount + vendor + temporal + pattern
- Configurable weights
- Dynamic thresholds
- Automatic HIGH/MEDIUM/LOW classification

**🔥 Autonomous Decision Engine:**
- **AUTO_APPROVE** for low-risk invoices (< 30%)
- **REVIEW_REQUIRED** for medium-risk (30-70%)
- **AUTO_REJECT** for high-risk invoices (> 70%)
- Confidence scores and reasoning for every decision
- Real-time automation without manual intervention

**Explainability:**
- Rule-based explanations (fast)
- Optional LLM-based explanations (detailed)
- Actionable recommendations

**📝 Professional Risk Explanation Generator:**

Automatically generates audit-ready explanations using only factual data:

```python
from llm_explainer import generate_risk_explanation

explanation = generate_risk_explanation(
    risk_score=85,
    deviation_percentage=65.0,
    bank_account_changed=True,
    duplicate_similarity_score=0.92,
    tax_mismatch=True
)

# Output:
# "This invoice presents significant fraud indicators with a risk score 
# of 85/100. Multiple critical fraud indicators were identified: amount 
# deviation of 65.0% from vendor's historical average, recent bank account 
# change, high similarity (0.92) to existing invoices, and tax calculation 
# inconsistency. The convergence of these anomalies requires immediate 
# attention to prevent potential financial loss. Recommended action: 
# immediate manual review and payment hold."
```

**Key Features:**
- ✅ **No Hallucination**: Uses only provided facts
- ✅ **Professional Tone**: Audit-ready language  
- ✅ **Consistent Format**: 3-4 sentences every time
- ✅ **Risk-Appropriate**: Recommendations match severity
- ✅ **Factor Transparency**: Clearly identifies contributing factors

**Generated Explanations Cover:**
1. Risk level and score
2. Specific fraud indicators detected
3. Why these patterns are concerning
4. Recommended actions (approve/review/reject)

## 📁 Project Structure

```
FinGuard-AI/
├── invoice_stream.py       # Real-time invoice stream generation
├── vendor_state.py         # Incremental vendor statistics
├── duplicate_detector.py   # Streaming duplicate detection (exact/fuzzy)
├── semantic_duplicates.py  # 🧠 Semantic duplicate detection (embeddings)
├── risk_engine.py          # Dynamic risk scoring
├── llm_explainer.py        # Real-time explanations + risk explanation generator
├── pipeline.py             # Main streaming orchestrator
├── streamlit_app.py        # Live dashboard
├── test_explanation.py     # Test suite for explanation generator
├── requirements.txt        # Minimal dependencies
├── README.md               # This file
├── .env.example            # Configuration template
├── .gitignore              # Git exclusions
└── output/                 # Auto-generated streaming outputs
    ├── high_risk_alerts.jsonl
    ├── autonomous_decisions.jsonl  # 🔥 Auto approve/review/reject
    ├── explanations.jsonl
    └── vendor_stats.jsonl
```

## 🔧 Configuration

### Environment Variables

Create `.env` file (optional):

```bash
# LLM Configuration (optional)
OPENAI_API_KEY=your-openai-key

# Pipeline Settings
OUTPUT_DIR=output
RISK_THRESHOLD=0.6
```

### Customize Risk Scoring

Edit `risk_engine.py`:

```python
weights = {
    "amount": 0.35,    # Amount-based risks
    "vendor": 0.25,    # Vendor behavior risks
    "temporal": 0.15,  # Time-based patterns
    "pattern": 0.25    # Fraud pattern matching
}
```

## 📈 Usage Examples

### Test Individual Modules

Each module is self-contained and testable:

```bash
# Test invoice stream
python invoice_stream.py

# Test vendor tracking (shows incremental aggregations)
python vendor_state.py

# Test duplicate detection
python duplicate_detector.py

# Test risk scoring
python risk_engine.py

# Test explanations
python llm_explainer.py
```

### Monitor Real-Time Output

```bash
# Watch alerts as they arrive
tail -f output/high_risk_alerts.jsonl

# View explanations
cat output/explanations.jsonl | jq .

# Check vendor statistics (auto-updating)
cat output/vendor_stats.jsonl | jq .
```

## 🎨 Dashboard Features

The Streamlit dashboard provides:

- **Summary Metrics**: Total alerts, high-risk count, average scores
- **Risk Distribution**: Visual breakdown of risk levels
- **Recent Alerts**: Real-time alert feed
- **Vendor Insights**: Top vendors by activity and amounts
- **Fraud Explanations**: Expandable details for each alert
- **Export Options**: Download alerts and stats as CSV
- **Auto-Refresh**: Configurable live updates

## 🔄 Data Flow

```
┌─────────────────┐
│ Invoice Stream  │ (invoice_stream.py)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Vendor Tracking │ (vendor_state.py)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Duplicate Check │ (duplicate_detector.py)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Risk Scoring   │ (risk_engine.py)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Explanations   │ (llm_explainer.py)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Output Files  │
│   + Dashboard   │shows **real-time updates** as invoices arrive:

- **Live Summary Metrics**: Auto-updating alert counts and risk scores
- **Risk Distribution**: Real-time charts
- **Recent Alerts**: Streaming alert feed
- **Vendor Insights**: Incremental vendor statistics
- **Fraud Explanations**: Expandable details with recommendations
- **Export Options**: Download current state as CSV
- **Auto-Refresh**: Configurable refresh intervals

## ⚡ Why Pathway?

This system leverages Pathway's unique capabilities:

1. **Incremental Computation**: Only new data is processed
2. **Stateful Streaming**: Maintains vendor statistics automatically
3. **Automatic Updates**: All tables update when new invoices arrive
4. **No Reprocessing**: Historical data never recomputed
5. **Event-Driven**: Pure streaming architecture

**Traditional Approach (Batch):**
```
New invoice → Load ALL data → Recompute ALL stats → Output
```

**Pathway Approach (Streaming):**
```
New invoice → Update ONLY affected stats → Output delta
```

## 🔐 Production Considerations

While this is a hackathon-ready demonstration, for production:

- Add proper authentication and authorization
- Implement data validation and sanitization
- Connect to real data sources (Kafka, databases)
- Add monitoring and alerting
- Implement proper error handling and logging
- Add comprehensive testing
- Consider horizontal scaling

## 🤝 Contributing

This project is built for **Hack For Green Bharat - Pathway Track**.

Contributions welcome:
- Enhanced fraud detection patterns
- Additional streaming transformations
- Dashboard improvements
- Documentation enhancements

## 📝 License

MIT License

## 🙏 Acknowledgments

Built with:
- **[Pathway](https://pathway.com/)** - Real-time streaming framework
- **[Streamlit](https://streamlit.io/)** - Dashboard framework
- **[OpenAI](https://openai.com/)** - Optional LLM explanations

---

## ⚠️ Important Notes

- ✅ **100% Streaming**: No batch processing
- ✅ **Event-Driven**: Auto-updates on new data
- ✅ **No Pandas in Core**: Uses Pathway tables only
- ✅ **Incremental**: No full dataset recomputation
- ✅ **Hackathon-Ready**: Minimal, modular, production-style

## 🎯 Getting Started Checklist

- [ ] Clone repository
- [ ] Install dependencies (`pip install -r requirements.txt`)
- [ ] Run pipeline (`python pipeline.py`)
- [ ] Launch dashboard (`streamlit run streamlit_app.py`)
- [ ] Watch real-time alerts appearing!

**The system automatically updates as new invoices arrive. No manual refresh needed!**