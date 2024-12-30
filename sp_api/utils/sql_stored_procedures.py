ORDER_SUMMARY_PROC = """
CREATE PROCEDURE SP_OrderSummary
    @StartDate DATETIME2,
    @EndDate DATETIME2
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        CONVERT(DATE, o.ProcessedDate) as OrderDate,
        COUNT(DISTINCT o.AmazonOrderId) as TotalOrders,
        COUNT(i.StagingId) as TotalItems,
        SUM(CAST(JSON_VALUE(i.RawData, '$.ItemPrice.Amount') as DECIMAL(10,2))) as TotalAmount
    FROM stage.SP_API_Orders o
    LEFT JOIN stage.SP_API_OrderItems i ON o.AmazonOrderId = i.AmazonOrderId
    WHERE o.ProcessedDate BETWEEN @StartDate AND @EndDate
    AND o.ValidationStatus = 'SUCCESS'
    GROUP BY CONVERT(DATE, o.ProcessedDate)
    ORDER BY OrderDate DESC;
END
"""

INVENTORY_AGING_PROC = """
CREATE PROCEDURE SP_InventoryAging
    @DaysThreshold INT = 90
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        i.SellerSKU,
        i.ASIN,
        JSON_VALUE(i.RawData, '$.ProductName') as ProductName,
        JSON_VALUE(i.RawData, '$.Quantity') as AvailableQuantity,
        DATEDIFF(DAY, i.ProcessedDate, GETDATE()) as DaysInInventory,
        i.ProcessedDate as LastUpdated
    FROM stage.SP_API_Inventory i
    WHERE i.ValidationStatus = 'SUCCESS'
    AND DATEDIFF(DAY, i.ProcessedDate, GETDATE()) >= @DaysThreshold
    ORDER BY DaysInInventory DESC;
END
"""

def create_stored_procedures(conn):
    """Create stored procedures if they don't exist"""
    cursor = conn.cursor()
    
    # Create SP_OrderSummary
    cursor.execute("""
    IF NOT EXISTS (
        SELECT * FROM sys.procedures WHERE name = 'SP_OrderSummary'
    )
    BEGIN
        EXEC('
        {}
        ')
    END
    """.format(ORDER_SUMMARY_PROC.replace("'", "''")))
    
    # Create SP_InventoryAging
    cursor.execute("""
    IF NOT EXISTS (
        SELECT * FROM sys.procedures WHERE name = 'SP_InventoryAging'
    )
    BEGIN
        EXEC('
        {}
        ')
    END
    """.format(INVENTORY_AGING_PROC.replace("'", "''")))
    
    conn.commit() 