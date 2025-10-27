# Databricks Delta Sharing Integration Guide

This guide shows how to connect Databricks to your deployed Fairgrounds Delta Share server and access the shared data.

## Prerequisites

1. **Databricks Workspace**: Access to a Databricks workspace (any cloud provider)
2. **Delta Share Server**: Your Azure-deployed Delta Share server running
3. **Bearer Token**: The authentication token from your deployment

## Step 1: Create Delta Share Profile File

In Databricks, you need to create a profile file that contains connection information to your Delta Share server.

### 1.1 Create the Profile JSON

Create a JSON file with your Delta Share server configuration:

```json
{
  "shareCredentialsVersion": 1,
  "endpoint": "http://fairgrounds-deltashare-development-deltashare.eastus.azurecontainer.io:8080",
  "bearerToken": "9b633f0dc26742388aeadda89b88e5f0"
}
```

### 1.2 Upload Profile to Databricks

**Option A: Upload via Databricks UI**
1. Go to your Databricks workspace
2. Navigate to **Data** → **Create** → **File Upload**
3. Upload the JSON file (e.g., name it `fairgrounds_deltashare_profile.json`)
4. Note the file path (usually `/FileStore/shared_uploads/your_email/fairgrounds_deltashare_profile.json`)

**Option B: Create via Notebook**
```python
# Create profile file directly in Databricks
profile_content = """{
  "shareCredentialsVersion": 1,
  "endpoint": "http://fairgrounds-deltashare-development-deltashare.eastus.azurecontainer.io:8080",
  "bearerToken": "9b633f0dc26742388aeadda89b88e5f0"
}"""

# Write to DBFS
dbutils.fs.put("/FileStore/fairgrounds_deltashare_profile.json", profile_content, overwrite=True)
```

## Step 2: Install Delta Sharing Client (if needed)

Most modern Databricks clusters include Delta Sharing support by default. If not, install it:

```python
%pip install delta-sharing
```

## Step 3: Connect to Delta Share

### 3.1 Load the Profile and List Shares

```python
import delta_sharing

# Load the profile
profile_file = "/dbfs/FileStore/fairgrounds_deltashare_profile.json"
client = delta_sharing.SharingClient(profile_file)

# List available shares
shares = client.list_shares()
print("Available shares:")
for share in shares:
    print(f"  - {share.name}")
```

### 3.2 List Schemas and Tables

```python
# List schemas in the first share
share_name = shares[0].name
schemas = client.list_schemas(delta_sharing.Share(name=share_name))
print(f"\nSchemas in {share_name}:")
for schema in schemas:
    print(f"  - {schema.name}")

# List tables in the first schema
schema_name = schemas[0].name
tables = client.list_tables(delta_sharing.Schema(name=schema_name, share=share_name))
print(f"\nTables in {schema_name}:")
for table in tables:
    print(f"  - {table.name}")
```

## Step 4: Load Data from Delta Share

### 4.1 Load Table as Pandas DataFrame

```python
# Load a specific table
table_url = f"{profile_file}#{share_name}.{schema_name}.customers"
df = delta_sharing.load_as_pandas(table_url)

# Display the data
print("Customer data:")
display(df)
```

### 4.2 Load Table as Spark DataFrame

```python
# Load as Spark DataFrame for larger datasets
spark_df = delta_sharing.load_as_spark(table_url)

# Show the data
spark_df.show()

# Get schema information
spark_df.printSchema()
```

### 4.3 Load All Tables

```python
# Load all available tables
tables_data = {}

for table in tables:
    table_url = f"{profile_file}#{share_name}.{schema_name}.{table.name}"
    print(f"Loading {table.name}...")
    
    try:
        df = delta_sharing.load_as_pandas(table_url)
        tables_data[table.name] = df
        print(f"  ✅ Loaded {len(df)} rows")
    except Exception as e:
        print(f"  ❌ Error loading {table.name}: {e}")

# Display all loaded tables
for table_name, df in tables_data.items():
    print(f"\n=== {table_name.upper()} ===")
    display(df.head())
```

## Step 5: Analyze the Data

### 5.1 Basic Analysis

```python
# Analyze customer data
customers_df = tables_data.get('customers')
if customers_df is not None:
    print("Customer Analysis:")
    print(f"  Total customers: {len(customers_df)}")
    print(f"  Unique states: {customers_df['state'].nunique()}")
    print(f"  States: {', '.join(customers_df['state'].unique())}")

# Analyze orders data
orders_df = tables_data.get('orders')
if orders_df is not None:
    print("\nOrder Analysis:")
    print(f"  Total orders: {len(orders_df)}")
    print(f"  Unique products: {orders_df['product_name'].nunique()}")
    print(f"  Order statuses: {', '.join(orders_df['status'].unique())}")
    print(f"  Total revenue: ${orders_df['price'].sum():.2f}")
```

### 5.2 Data Visualization

```python
import matplotlib.pyplot as plt
import seaborn as sns

# Plot order status distribution
if orders_df is not None:
    plt.figure(figsize=(10, 6))
    
    # Order status counts
    plt.subplot(1, 2, 1)
    orders_df['status'].value_counts().plot(kind='bar')
    plt.title('Order Status Distribution')
    plt.xlabel('Status')
    plt.ylabel('Count')
    plt.xticks(rotation=45)
    
    # Revenue by product
    plt.subplot(1, 2, 2)
    product_revenue = orders_df.groupby('product_name')['price'].sum().sort_values(ascending=False)
    product_revenue.plot(kind='bar')
    plt.title('Revenue by Product')
    plt.xlabel('Product')
    plt.ylabel('Revenue ($)')
    plt.xticks(rotation=45)
    
    plt.tight_layout()
    plt.show()
```

## Step 6: Join Data Across Tables

```python
# Join customers and orders data
if customers_df is not None and orders_df is not None:
    # Merge customer and order data
    customer_orders = orders_df.merge(
        customers_df[['customer_id', 'name', 'city', 'state']], 
        on='customer_id', 
        how='left'
    )
    
    print("Joined Customer-Order Data:")
    display(customer_orders.head())
    
    # Analyze orders by state
    state_analysis = customer_orders.groupby('state').agg({
        'order_id': 'count',
        'price': 'sum',
        'quantity': 'sum'
    }).round(2)
    
    print("\nOrders by State:")
    display(state_analysis)
```

## Step 7: Create Databricks Tables (Optional)

You can create permanent Databricks tables from the Delta Share data:

```python
# Create temporary views
for table_name, df in tables_data.items():
    spark_df = spark.createDataFrame(df)
    spark_df.createOrReplaceTempView(f"fairgrounds_{table_name}")
    print(f"Created view: fairgrounds_{table_name}")

# Now you can use SQL
display(spark.sql("""
    SELECT 
        c.state,
        COUNT(o.order_id) as total_orders,
        SUM(o.price * o.quantity) as total_revenue
    FROM fairgrounds_customers c
    JOIN fairgrounds_orders o ON c.customer_id = o.customer_id
    GROUP BY c.state
    ORDER BY total_revenue DESC
"""))
```

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Verify bearer token is correct
   - Ensure token includes full string: `9b633f0dc26742388aeadda89b88e5f0`

2. **Connection Errors**
   - Check Azure Container Instance is running
   - Verify URL is accessible: `http://fairgrounds-deltashare-development-deltashare.eastus.azurecontainer.io:8080`

3. **Profile File Issues**
   - Ensure JSON format is valid
   - Check file path in DBFS is correct

### Verify Connection

```python
# Test connection
try:
    shares = client.list_shares()
    print(f"✅ Successfully connected! Found {len(shares)} shares.")
except Exception as e:
    print(f"❌ Connection failed: {e}")
```

## Security Considerations

1. **Bearer Token Security**
   - Store bearer token in Databricks Secrets instead of hardcoding
   - Rotate tokens regularly

2. **Network Security**
   - Consider using private endpoints for production
   - Implement IP allowlisting if needed

3. **Access Control**
   - Use Databricks Unity Catalog for data governance
   - Implement proper user access controls

## Next Steps

1. **Set up automated data pipelines** using Databricks Jobs
2. **Create dashboards** using Databricks SQL
3. **Implement data quality checks** on shared data
4. **Set up monitoring** for data freshness and availability

---

This integration demonstrates the power of Delta Sharing to securely share data across different platforms while maintaining control and governance over your data assets.