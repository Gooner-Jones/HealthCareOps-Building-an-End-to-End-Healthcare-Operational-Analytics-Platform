# ============================================================
# CELL 1 : Create a text widget for catalog name and retrieve its value
# ============================================================
dbutils.widgets.text("catalog_name", "hospital_analytics", "Catalog Name")
catalog_name = dbutils.widgets.get("catalog_name")
