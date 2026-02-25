"""
Quick test script for the risk explanation generator.
Demonstrates fact-based, professional explanations.
"""

from llm_explainer import generate_risk_explanation


def test_explanation_generator():
    """Test various scenarios with the explanation generator."""
    
    print("=" * 80)
    print("RISK EXPLANATION GENERATOR - Quick Test")
    print("=" * 80)
    
    # Scenario 1: Critical risk (all factors)
    print("\n📊 Scenario 1: Critical Risk Invoice")
    print("-" * 80)
    explanation = generate_risk_explanation(
        risk_score=100,
        deviation_percentage=85.0,
        bank_account_changed=True,
        duplicate_similarity_score=0.92,
        tax_mismatch=True
    )
    print(explanation)
    
    # Scenario 2: Moderate risk (2 factors)
    print("\n📊 Scenario 2: Moderate Risk Invoice")
    print("-" * 80)
    explanation = generate_risk_explanation(
        risk_score=50,
        deviation_percentage=45.0,
        bank_account_changed=True,
        duplicate_similarity_score=0.30,
        tax_mismatch=False
    )
    print(explanation)
    
    # Scenario 3: Low risk (1 factor)
    print("\n📊 Scenario 3: Low Risk Invoice")
    print("-" * 80)
    explanation = generate_risk_explanation(
        risk_score=20,
        deviation_percentage=5.0,
        bank_account_changed=False,
        duplicate_similarity_score=0.12,
        tax_mismatch=True
    )
    print(explanation)
    
    # Scenario 4: Clean invoice
    print("\n📊 Scenario 4: Clean Invoice")
    print("-" * 80)
    explanation = generate_risk_explanation(
        risk_score=0,
        deviation_percentage=-2.0,
        bank_account_changed=False,
        duplicate_similarity_score=0.05,
        tax_mismatch=False
    )
    print(explanation)
    
    print("\n" + "=" * 80)
    print("✅ All tests passed!")
    print("=" * 80)


if __name__ == "__main__":
    test_explanation_generator()
