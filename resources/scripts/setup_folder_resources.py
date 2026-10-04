import os
import sys

# Define base paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

ENTITIES = [
    "customers",
    "addresses",
    "subscriptions",
    "products",
    "orders",
    "order_items",
    "payments",
    "support_tickets"
]

SUBFOLDERS = ["initial", "incremental", "archive"]

LAYERS = {
    "staging/source_files": ENTITIES,
    "bronze": ENTITIES + ["cdc_control"],
    "silver": ENTITIES,
    "gold": [
        "dim_customer",
        "dim_product",
        "customer_scd2",
        "agg_customer_360",
        "fact_orders",
        "fact_order_items",
        "fact_payments",
        "fact_support_tickets"
    ],
    "control": ["watermark", "audit_log", "cdc_control"],
    "checkpoints": ENTITIES
}

def create_folder_structure():
    created_count = 0
    print(f"Initializing folder resources under: {DATA_DIR}\n" + "="*60)
    
    for layer, components in LAYERS.items():
        for comp in components:
            if layer == "staging/source_files":
                for sub in SUBFOLDERS:
                    folder_path = os.path.join(DATA_DIR, layer, comp, sub)
                    os.makedirs(folder_path, exist_ok=True)
                    gitkeep = os.path.join(folder_path, ".gitkeep")
                    if not os.path.exists(gitkeep):
                        open(gitkeep, "w").close()
                    print(f"[OK] Created staging folder: data/{layer}/{comp}/{sub}")
                    created_count += 1
            else:
                folder_path = os.path.join(DATA_DIR, layer, comp)
                os.makedirs(folder_path, exist_ok=True)
                gitkeep = os.path.join(folder_path, ".gitkeep")
                if not os.path.exists(gitkeep):
                    open(gitkeep, "w").close()
                print(f"[OK] Created layer folder: data/{layer}/{comp}")
                created_count += 1
                
    print("="*60)
    print(f"Successfully initialized {created_count} folder resources.")

if __name__ == "__main__":
    create_folder_structure()
