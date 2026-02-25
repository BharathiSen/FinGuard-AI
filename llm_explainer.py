"""
LLM-based explanation generator for fraud detection decisions.
Provides human-readable explanations for risk scores and alerts.
"""

import pathway as pw
from typing import Optional, Dict
import json


class ExplanationSchema(pw.Schema):
    """Schema for fraud explanations."""
    invoice_id: str
    explanation: str
    key_factors: str
    recommendations: str


def create_explanation_prompt(invoice_data: Dict) -> str:
    """
    Create a prompt for LLM to generate fraud explanation.
    
    Args:
        invoice_data: Dictionary with invoice and risk information
    
    Returns:
        Formatted prompt string
    """
    
    prompt = f"""You are a financial fraud detection expert. Analyze the following invoice and explain why it was flagged as potentially fraudulent.

Invoice Details:
- Invoice ID: {invoice_data.get('invoice_id', 'N/A')}
- Vendor ID: {invoice_data.get('vendor_id', 'N/A')}
- Amount: ${invoice_data.get('amount', 0):,.2f}
- Description: {invoice_data.get('description', 'N/A')}
- Category: {invoice_data.get('category', 'N/A')}
- Payment Method: {invoice_data.get('payment_method', 'N/A')}

Risk Assessment:
- Risk Score: {invoice_data.get('risk_score', 0):.2f} ({invoice_data.get('risk_level', 'UNKNOWN')})
- Amount Risk: {invoice_data.get('amount_risk', 0):.2f}
- Vendor Risk: {invoice_data.get('vendor_risk', 0):.2f}
- Pattern Risk: {invoice_data.get('pattern_risk', 0):.2f}

Vendor Historical Context:
- Total Previous Invoices: {invoice_data.get('vendor_total_invoices', 0)}
- Average Amount: ${invoice_data.get('vendor_avg_amount', 0):,.2f}
- This Invoice vs Average: {invoice_data.get('amount_vs_avg_ratio', 1):.1f}x

Provide a concise explanation (2-3 sentences) covering:
1. The primary fraud indicators
2. Why these patterns are suspicious
3. Recommended next steps

Keep the explanation clear and actionable for financial reviewers."""
    
    return prompt


def generate_rule_based_explanation(risk_data: pw.Table) -> pw.Table:
    """
    Generate rule-based explanations without LLM (faster, deterministic).
    
    Args:
        risk_data: Pathway Table with risk scores and factors
    
    Returns:
        Pathway Table with explanations
    """
    
    # Build explanation based on risk factors
    explanations = risk_data.select(
        invoice_id=pw.this.invoice_id,
        vendor_id=pw.this.vendor_id,
        risk_score=pw.this.risk_score,
        risk_level=pw.this.risk_level,
        # Primary factors
        primary_factor=pw.if_else(
            pw.this.amount_risk > pw.this.vendor_risk,
            pw.if_else(
                pw.this.amount_risk > pw.this.pattern_risk,
                "amount",
                "pattern"
            ),
            pw.if_else(
                pw.this.vendor_risk > pw.this.pattern_risk,
                "vendor",
                "pattern"
            )
        )
    )
    
    # Create explanation text
    explanations = explanations.select(
        invoice_id=pw.this.invoice_id,
        vendor_id=pw.this.vendor_id,
        risk_score=pw.this.risk_score,
        risk_level=pw.this.risk_level,
        explanation=pw.if_else(
            pw.this.risk_level == "HIGH",
            "This invoice has been flagged as HIGH RISK due to multiple suspicious indicators. " +
            pw.if_else(
                pw.this.primary_factor == "amount",
                "The invoice amount is significantly unusual compared to historical patterns. ",
                pw.if_else(
                    pw.this.primary_factor == "vendor",
                    "The vendor exhibits suspicious behavior patterns or limited history. ",
                    "The invoice matches known fraud patterns or duplicates. "
                )
            ) +
            "Immediate manual review is recommended.",
            pw.if_else(
                pw.this.risk_level == "MEDIUM",
                "This invoice shows MEDIUM RISK indicators. " +
                "While not immediately suspicious, it warrants closer inspection during routine review.",
                "This invoice appears to be LOW RISK with normal patterns consistent with vendor history."
            )
        ),
        key_factors=pw.if_else(
            pw.this.primary_factor == "amount",
            "Unusual amount, Deviation from vendor average",
            pw.if_else(
                pw.this.primary_factor == "vendor",
                "New or suspicious vendor, Limited transaction history",
                "Pattern matching, Potential duplicate"
            )
        ),
        recommendations=pw.if_else(
            pw.this.risk_level == "HIGH",
            "1. Verify vendor legitimacy, 2. Contact vendor directly, 3. Review supporting documentation, 4. Hold payment pending investigation",
            pw.if_else(
                pw.this.risk_level == "MEDIUM",
                "1. Verify invoice details, 2. Confirm with requesting department, 3. Review vendor history",
                "1. Process normally, 2. Periodic audit recommended"
            )
        )
    )
    
    return explanations


def generate_llm_explanation(
    risk_data: pw.Table,
    llm_provider: str = "openai",
    model: str = "gpt-4o-mini",
    use_streaming: bool = True
) -> pw.Table:
    """
    Generate LLM-based explanations using OpenAI or other providers.
    
    Args:
        risk_data: Pathway Table with risk scores and factors
        llm_provider: LLM provider ("openai", "anthropic", "local")
        model: Model name
        use_streaming: Whether to use streaming responses
    
    Returns:
        Pathway Table with LLM-generated explanations
    """
    
    try:
        import pathway.xpacks.llm as llm
        
        # Create prompt for each invoice
        prompts = risk_data.select(
            invoice_id=pw.this.invoice_id,
            prompt=pw.apply(
                lambda inv_id, vend_id, amt, desc, cat, pm, rs, rl, ar, vr, pr, vti, vaa, ratio: 
                create_explanation_prompt({
                    'invoice_id': inv_id,
                    'vendor_id': vend_id,
                    'amount': amt,
                    'description': desc,
                    'category': cat,
                    'payment_method': pm,
                    'risk_score': rs,
                    'risk_level': rl,
                    'amount_risk': ar,
                    'vendor_risk': vr,
                    'pattern_risk': pr,
                    'vendor_total_invoices': vti,
                    'vendor_avg_amount': vaa,
                    'amount_vs_avg_ratio': ratio
                }),
                pw.this.invoice_id,
                pw.this.vendor_id,
                pw.this.amount,
                pw.this.description,
                pw.this.category,
                pw.this.payment_method,
                pw.this.risk_score,
                pw.this.risk_level,
                pw.this.amount_risk,
                pw.this.vendor_risk,
                pw.this.pattern_risk,
                pw.this.vendor_total_invoices,
                pw.this.vendor_avg_amount,
                pw.this.amount_vs_avg_ratio
            )
        )
        
        # Call LLM
        if llm_provider == "openai":
            chat = llm.llms.OpenAIChat(
                model=model,
                temperature=0.3,  # Lower temperature for consistent explanations
                max_tokens=300
            )
            
            explanations = prompts.select(
                invoice_id=pw.this.invoice_id,
                explanation=chat(pw.this.prompt)
            )
        else:
            # Fallback to rule-based
            return generate_rule_based_explanation(risk_data)
        
        # Parse and structure the explanation
        explanations = explanations.select(
            invoice_id=pw.this.invoice_id,
            explanation=pw.this.explanation,
            key_factors="LLM-generated analysis",
            recommendations="See explanation for details"
        )
        
        return explanations
    
    except Exception as e:
        # Fallback to rule-based explanations if LLM fails
        print(f"LLM explanation failed, using rule-based: {e}")
        return generate_rule_based_explanation(risk_data)


def create_alert_summary(explanations: pw.Table) -> pw.Table:
    """
    Create a summary of alerts for monitoring dashboard.
    
    Args:
        explanations: Pathway Table with explanations
    
    Returns:
        Pathway Table with alert summaries
    """
    
    # Count alerts by risk level
    alert_summary = explanations.groupby(explanations.risk_level).reduce(
        risk_level=explanations.risk_level,
        count=pw.reducers.count(),
        total_amount=pw.reducers.sum(explanations.amount) if 'amount' in explanations else 0
    )
    
    return alert_summary


def format_explanation_for_display(explanations: pw.Table) -> pw.Table:
    """
    Format explanations for user-friendly display.
    
    Args:
        explanations: Pathway Table with explanations
    
    Returns:
        Pathway Table with formatted output
    """
    
    formatted = explanations.select(
        invoice_id=pw.this.invoice_id,
        vendor_id=pw.this.vendor_id,
        risk_level=pw.this.risk_level,
        risk_score=pw.cast(str, pw.this.risk_score),
        summary=pw.cast(str, 
            "🚨 " + pw.this.risk_level + " RISK ALERT\n\n" +
            "Invoice: " + pw.this.invoice_id + "\n" +
            "Vendor: " + pw.this.vendor_id + "\n" +
            "Risk Score: " + pw.cast(str, pw.this.risk_score) + "\n\n" +
            "Explanation:\n" + pw.this.explanation + "\n\n" +
            "Key Factors: " + pw.this.key_factors + "\n\n" +
            "Recommendations:\n" + pw.this.recommendations
        )
    )
    
    return formatted


def generate_batch_report(
    explanations: pw.Table,
    time_window: str = "last_24h"
) -> pw.Table:
    """
    Generate a batch report of all flagged invoices.
    
    Args:
        explanations: Pathway Table with explanations
        time_window: Time window for report
    
    Returns:
        Pathway Table with report data
    """
    
    # Aggregate statistics
    report = explanations.groupby().reduce(
        total_flagged=pw.reducers.count(),
        high_risk_count=pw.reducers.count(pw.if_else(
            explanations.risk_level == "HIGH",
            1,
            None
        )),
        medium_risk_count=pw.reducers.count(pw.if_else(
            explanations.risk_level == "MEDIUM",
            1,
            None
        )),
        avg_risk_score=pw.reducers.avg(explanations.risk_score)
    )
    
    return report


if __name__ == "__main__":
    # Example usage
    from invoice_stream import generate_invoice_stream
    from vendor_state import track_vendor_state, enrich_with_vendor_context
    from risk_engine import compute_composite_risk_score, filter_high_risk_invoices
    
    print("Testing LLM explainer...")
    
    # Generate sample invoices
    invoices = generate_invoice_stream(num_invoices=50)
    
    # Track vendor statistics
    vendor_stats = track_vendor_state(invoices)
    
    # Enrich invoices
    enriched_invoices = enrich_with_vendor_context(invoices, vendor_stats)
    
    # Compute risk scores
    risk_scores = compute_composite_risk_score(enriched_invoices)
    
    # Generate explanations
    explanations = generate_rule_based_explanation(risk_scores)
    
    # Display high-risk explanations
    high_risk_explanations = explanations.filter(pw.this.risk_level == "HIGH")
    
    print("\nHigh Risk Explanations:")
    pw.debug.compute_and_print(high_risk_explanations)
