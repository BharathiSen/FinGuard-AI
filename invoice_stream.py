"""
Simulated live invoice ingestion using Pathway streaming.
Generates synthetic invoice data to simulate real-time streaming.
"""

import pathway as pw
import random
from datetime import datetime, timedelta
from typing import Optional


class InvoiceSchema(pw.Schema):
    """Schema for invoice streaming data."""
    invoice_id: str
    vendor_id: str
    amount: float
    tax: float
    bank_account: str
    timestamp: str
    description: str
    category: str
    payment_method: str


def generate_invoice_stream(
    num_invoices: Optional[int] = None,
    interval_ms: int = 5000  # 5 seconds between invoices
) -> pw.Table:
    """
    Generate a simulated live stream of invoices using Pathway.
    
    Emits a new invoice every 5 seconds (configurable) with event-driven updates.
    Each invoice automatically triggers downstream processing in the pipeline.
    
    Args:
        num_invoices: Number of invoices to generate (None for infinite stream)
        interval_ms: Interval between invoice generation in milliseconds (default: 5000ms = 5s)
    
    Returns:
        Pathway Table with invoice data (auto-updating)
    """
    
    # Sample data for realistic invoice generation
    vendors = [
        "VND-001", "VND-002", "VND-003", "VND-004", "VND-005",
        "VND-006", "VND-007", "VND-008", "VND-009", "VND-010"
    ]
    
    # Bank accounts mapped to vendors (for duplicate detection)
    vendor_bank_accounts = {
        "VND-001": "ACC-1234567890",
        "VND-002": "ACC-2345678901",
        "VND-003": "ACC-3456789012",
        "VND-004": "ACC-4567890123",
        "VND-005": "ACC-5678901234",
        "VND-006": "ACC-6789012345",
        "VND-007": "ACC-7890123456",
        "VND-008": "ACC-8901234567",
        "VND-009": "ACC-9012345678",
        "VND-010": "ACC-0123456789"
    }
    
    categories = [
        "Office Supplies", "IT Equipment", "Consulting", 
        "Marketing", "Travel", "Utilities", "Maintenance"
    ]
    
    payment_methods = ["Wire Transfer", "Check", "Credit Card", "ACH"]
    
    descriptions = [
        "Monthly service fee",
        "Hardware procurement",
        "Software licensing",
        "Consulting services Q1",
        "Office furniture",
        "Cloud infrastructure",
        "Professional services",
        "Maintenance contract",
        "Marketing campaign",
        "Employee reimbursement"
    ]
    
    def generate_invoices():
        """
        Generator function for live invoice data.
        
        This is a lazy generator - Pathway's runtime controls when to pull data.
        Each yield produces a new invoice that triggers downstream updates.
        """
        count = 0
        while num_invoices is None or count < num_invoices:
            # Generate potentially fraudulent patterns occasionally
            is_suspicious = random.random() < 0.15  # 15% suspicious invoices
            
            vendor = random.choice(vendors)
            
            # Suspicious invoices might have unusual amounts or patterns
            if is_suspicious:
                amount = random.choice([
                    round(random.uniform(9500, 9999), 2),  # Just under threshold
                    round(random.uniform(50000, 100000), 2),  # Unusually high
                ])
                # Fraudulent invoices might use wrong bank account
                if random.random() < 0.3:  # 30% of suspicious use wrong account
                    bank_account = random.choice(list(vendor_bank_accounts.values()))
                else:
                    bank_account = vendor_bank_accounts[vendor]
            else:
                amount = round(random.uniform(100, 25000), 2)
                bank_account = vendor_bank_accounts[vendor]
            
            # Calculate tax (realistic rates: 5-10%)
            tax_rate = random.choice([0.05, 0.07, 0.08, 0.10])
            tax = round(amount * tax_rate, 2)
            
            invoice_data = {
                "invoice_id": f"INV-{datetime.now().strftime('%Y%m%d')}-{count:06d}",
                "vendor_id": vendor,
                "amount": amount,
                "tax": tax,
                "bank_account": bank_account,
                "timestamp": datetime.now().isoformat(),
                "description": random.choice(descriptions),
                "category": random.choice(categories),
                "payment_method": random.choice(payment_methods)
            }
            
            yield invoice_data
            count += 1
    
    # Create a Pathway streaming table from the generator
    # Pathway's runtime pulls from the generator and triggers downstream updates
    invoice_table = pw.demo.generate_custom_stream(
        generate_invoices,
        schema=InvoiceSchema,
        nb_rows=num_invoices,
        input_rate=1000 // interval_ms if interval_ms > 0 else 1
    )
    
    return invoice_table


if __name__ == "__main__":
    # Example usage - simulated live invoice stream
    print("Starting live invoice stream simulation...")
    print("⏱️  Emitting new invoice every 5 seconds...")
    print("🔄 Each invoice automatically triggers downstream updates\n")
    
    # Generate a finite stream for testing (10 invoices at 5-second intervals)
    invoices = generate_invoice_stream(num_invoices=10, interval_ms=5000)
    
    # Display the stream
    pw.debug.compute_and_print(invoices)
