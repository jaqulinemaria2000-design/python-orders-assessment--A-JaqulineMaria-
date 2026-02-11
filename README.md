# python-orders-assessment--A-JaqulineMaria-
**How to Run**
**Run the demo (built into the script):**

python orders_program.py

**To run programmatically:Using compute_report()**

from orders_program import compute_report
 
with open("sample_input_20_lines.txt") as f:
    report = compute_report(f, top_n=5)
 
print(report)

**Input Format Specification
Each line should follow:**

order_id,timestamp,customer_id,item_id,qty,price,currency,status,coupon_code?
