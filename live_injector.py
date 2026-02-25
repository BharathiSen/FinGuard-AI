"""
Live Invoice Injector — Windows-native Pathway simulator.
Generates a new invoice every 5 seconds, scores it, writes to JSONL.
Dashboard auto-refreshes and picks up the new records.
"""

import json, random, time, os
from datetime import datetime
from pathlib import Path

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

DECISIONS_FILE = OUTPUT_DIR / "autonomous_decisions.jsonl"
ALERTS_FILE    = OUTPUT_DIR / "high_risk_alerts.jsonl"

VENDORS = [f"VND-{i:03d}" for i in range(1, 11)]
VENDOR_BANK = {v: f"ACC-{random.randint(1000000000,9999999999)}" for v in VENDORS}
CATEGORIES   = ["Office Supplies","IT Equipment","Consulting","Marketing","Travel","Utilities","Maintenance"]
DESCRIPTIONS = ["Monthly service fee","Hardware procurement","Software licensing",
                "Consulting services","Office furniture","Cloud infrastructure",
                "Professional services","Maintenance contract","Marketing campaign"]
PAYMENTS     = ["Wire Transfer","Check","Credit Card","ACH"]

counter = int(datetime.now().timestamp())   # unique seed

def risk_score(invoice: dict) -> float:
    score = 0.0
    amt = invoice["amount"]

    # Amount thresholds
    if amt > 80000:   score += 0.40
    elif amt > 40000: score += 0.20
    elif amt > 9000 and amt < 10000: score += 0.30   # just-under threshold

    # Suspicious round amounts
    if amt % 1000 == 0: score += 0.10

    # Wrong bank account
    if invoice["bank_account"] != VENDOR_BANK[invoice["vendor_id"]]:
        score += 0.45

    # Unusual hours (midnight-5am)
    hour = datetime.fromisoformat(invoice["timestamp"]).hour
    if hour < 5: score += 0.20

    # Random low-level noise
    score += random.uniform(0.0, 0.15)

    return min(round(score, 4), 1.0)


def decision(score: float) -> str:
    if score >= 0.65: return "AUTO_REJECT"
    if score >= 0.35: return "REVIEW_REQUIRED"
    return "AUTO_APPROVE"


def generate_invoice() -> dict:
    global counter
    counter += 1
    suspicious = random.random() < 0.18
    vendor = random.choice(VENDORS)

    if suspicious:
        amount = random.choice([
            round(random.uniform(9500, 9999), 2),
            round(random.uniform(50000, 100000), 2),
        ])
        bank = (random.choice(list(VENDOR_BANK.values()))
                if random.random() < 0.4 else VENDOR_BANK[vendor])
    else:
        amount = round(random.uniform(200, 30000), 2)
        bank   = VENDOR_BANK[vendor]

    tax = round(amount * random.uniform(0.05, 0.18), 2)
    ts  = datetime.now().isoformat()

    inv = {
        "invoice_id":     f"INV-{datetime.now().strftime('%Y%m%d')}-{counter:06d}",
        "vendor_id":      vendor,
        "amount":         amount,
        "tax":            tax,
        "bank_account":   bank,
        "timestamp":      ts,
        "description":    random.choice(DESCRIPTIONS),
        "category":       random.choice(CATEGORIES),
        "payment_method": random.choice(PAYMENTS),
    }

    inv["risk_score"] = risk_score(inv)
    inv["risk_level"] = ("HIGH" if inv["risk_score"] >= 0.65
                         else "MEDIUM" if inv["risk_score"] >= 0.35 else "LOW")
    inv["decision"]   = decision(inv["risk_score"])
    return inv


def append_jsonl(path: Path, record: dict):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def main():
    print("=" * 60)
    print("  FinGuard AI — Live Invoice Injector")
    print("  New invoice every 5 seconds -> output/*.jsonl")
    print("  Dashboard at http://localhost:8502  auto-refreshes")
    print("  Press Ctrl+C to stop")
    print("=" * 60)

    total = 0
    while True:
        inv   = generate_invoice()
        append_jsonl(DECISIONS_FILE, inv)
        if inv["risk_score"] >= 0.60:
            append_jsonl(ALERTS_FILE, inv)

        total += 1
        flag = "[HIGH]" if inv["risk_level"] == "HIGH" else (
               "[MED] " if inv["risk_level"] == "MEDIUM" else "[LOW] ")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] #{total:>4}  "
              f"{inv['invoice_id']}  {inv['vendor_id']}  "
              f"Rs{inv['amount']:>10,.0f}  risk={inv['risk_score']:.2f}  "
              f"{flag}  {inv['decision']}")
        time.sleep(5)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInjector stopped.")
