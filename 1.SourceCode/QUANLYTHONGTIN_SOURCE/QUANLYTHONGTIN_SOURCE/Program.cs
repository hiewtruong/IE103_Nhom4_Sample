using Couchbase;
using Couchbase.KeyValue;
using Couchbase.Query;
using Newtonsoft.Json;
using QUANLYTHONGTIN_SOURCE.Models;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace QUANLYTHONGTIN_SOURCE
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var formatTime = "HH:mm:ss tt";
            string currentDicrectory = Directory.GetParent(Directory.GetCurrentDirectory()).Parent.FullName;
            string jsonFilePath = "sampleData.json";
            string filePath = Path.Combine(currentDicrectory, jsonFilePath);
            string jsonString = File.ReadAllText(filePath);
            List<Product> products = JsonConvert.DeserializeObject<List<Product>>(jsonString);
            Console.WriteLine("QUAN LY THONG TIN NHOM 4 - COUCHBASE");
            Console.WriteLine("-------- START --------");
            Console.WriteLine("");
            Console.WriteLine("1: INSERT DATA");
            Console.WriteLine("");
            ProcessRunSQLDatabase(products, formatTime);
            Console.WriteLine("--------");
            ProcessRunCouchBaseServer(products, formatTime);
            Console.WriteLine("");
            Console.WriteLine("");
            Console.WriteLine("2: GET ALL");
            Console.WriteLine("");
            ProcessRunGetDataSQLDatabaseGetAll(formatTime);
            Console.WriteLine("--------");
            await ProcessRunGetDataCouchBaseAsyncGetAll(formatTime);
            Console.WriteLine("");
            Console.WriteLine("");
            Console.WriteLine("3: GET DATA WITH CONDITION PRODUCT DETAILS CATEGORY LIKE 'Washing' OR TYPE LIKE 'techno'");
            Console.WriteLine("");
            ProcessRunGetDataSQLDatabase(formatTime);
            Console.WriteLine("--------");
            await ProcessRunGetDataCouchBaseAsync(formatTime);
            Console.WriteLine("-------- END --------");
            Console.ReadKey();
        }

        static void ProcessRunSQLDatabase(List<Product> products, string formatTime)
        {
            DateTime startTime = DateTime.Now;
            Console.WriteLine(string.Format("Start Time SQL: {0}", startTime.ToString(formatTime)));
            SqlConnection conn = new SqlConnection("Data Source=localhost\\SQLEXPRESS;Initial Catalog=QUANLYTHONGTIN_SQL_NHOM4;Persist Security Info=True;User ID=sa;Password=123456aA@");
            try
            {
                DateTime now = DateTime.Now;
                conn.Open();
                foreach (Product product in products)
                {
                    Guid productId = Guid.NewGuid();

                    String queryProduct = "INSERT INTO dbo.Product (Id,Name,SKU,Category,Description,[Delete],CreatedBy,CreatedDate,UpdatedBy,UpdatedDate) " +
                                   "VALUES (@Id,@Name,@SKU,@Category,@Description,@Delete,@CreatedBy,@CreatedDate,@UpdatedBy,@UpdatedDate)";
                    SqlCommand commandProduct = new SqlCommand(queryProduct, conn);
                    commandProduct.Parameters.AddWithValue("@Id", productId);
                    commandProduct.Parameters.AddWithValue("@Name", product.Name);
                    commandProduct.Parameters.AddWithValue("@SKU", product.SKU);
                    commandProduct.Parameters.AddWithValue("@Category", product.Category);
                    commandProduct.Parameters.AddWithValue("@Description", product.Description);
                    commandProduct.Parameters.AddWithValue("@Delete", Convert.ToInt32(product.Delete));
                    commandProduct.Parameters.AddWithValue("@CreatedBy", product.CreatedBy);
                    commandProduct.Parameters.AddWithValue("@CreatedDate", now);
                    commandProduct.Parameters.AddWithValue("@UpdatedBy", product.UpdatedBy);
                    commandProduct.Parameters.AddWithValue("@UpdatedDate", now);

                    int resultProduct = commandProduct.ExecuteNonQuery();

                    if (resultProduct < 0)
                    {
                        throw new Exception("Error inserting product data into Database!");
                    }

                    foreach (var productDetail in product.ProductDetails)
                    {
                        Guid productDetailId = Guid.NewGuid();
                        String queryProductDetails = "INSERT INTO dbo.ProductDetail (Id,ProductId,Type,Category,Description,[Delete],CreatedBy,CreatedDate,UpdatedBy,UpdatedDate) " +
                                  "VALUES (@Id,@ProductId,@Type,@Category,@Description,@Delete,@CreatedBy,@CreatedDate,@UpdatedBy,@UpdatedDate)";
                        SqlCommand commandProductDetails = new SqlCommand(queryProductDetails, conn);
                        commandProductDetails.Parameters.AddWithValue("@Id", productDetailId);
                        commandProductDetails.Parameters.AddWithValue("@ProductId", productId);
                        commandProductDetails.Parameters.AddWithValue("@Type", productDetail.Type);
                        commandProductDetails.Parameters.AddWithValue("@Category", productDetail.Category);
                        commandProductDetails.Parameters.AddWithValue("@Description", productDetail.Description);
                        commandProductDetails.Parameters.AddWithValue("@Delete", Convert.ToInt32(productDetail.Delete));
                        commandProductDetails.Parameters.AddWithValue("@CreatedBy", productDetail.CreatedBy);
                        commandProductDetails.Parameters.AddWithValue("@CreatedDate", now);
                        commandProductDetails.Parameters.AddWithValue("@UpdatedBy", productDetail.UpdatedBy);
                        commandProductDetails.Parameters.AddWithValue("@UpdatedDate", now);
                        int resultProductDetails = commandProductDetails.ExecuteNonQuery();

                        if (resultProduct < 0)
                        {
                            throw new Exception("Error inserting product detail data into Database!");
                        }

                    }

                    foreach (var productImage in product.ProductImages)
                    {
                        Guid productImageId = Guid.NewGuid();
                        String queryProductImage = "INSERT INTO dbo.ProductImage (Id,ProductId,Url,[Delete],CreatedBy,CreatedDate,UpdatedBy,UpdatedDate) " +
                                  "VALUES (@Id,@ProductId,@Url,@Delete,@CreatedBy,@CreatedDate,@UpdatedBy,@UpdatedDate)";
                        SqlCommand commandProductImage = new SqlCommand(queryProductImage, conn);
                        commandProductImage.Parameters.AddWithValue("@Id", productImageId);
                        commandProductImage.Parameters.AddWithValue("@ProductId", productId);
                        commandProductImage.Parameters.AddWithValue("@Url", productImage.Url);
                        commandProductImage.Parameters.AddWithValue("@Delete", Convert.ToInt32(productImage.Delete));
                        commandProductImage.Parameters.AddWithValue("@CreatedBy", productImage.CreatedBy);
                        commandProductImage.Parameters.AddWithValue("@CreatedDate", now);
                        commandProductImage.Parameters.AddWithValue("@UpdatedBy", productImage.UpdatedBy);
                        commandProductImage.Parameters.AddWithValue("@UpdatedDate", now);
                        int resultProductDetails = commandProductImage.ExecuteNonQuery();

                        if (resultProduct < 0)
                        {
                            throw new Exception("Error inserting product image data into Database!");
                        }
                    }

                }
                conn.Close();
            }
            catch (Exception ex)
            {
                conn.Close();
                Console.WriteLine(ex.Message);
                Console.ReadKey();
            }

            DateTime endTime = DateTime.Now;
            var processSecond = endTime - startTime;
            double roundedSeconds = Math.Round(processSecond.TotalSeconds);
            Console.WriteLine(string.Format("End Time SQL: {0}", endTime.ToString(formatTime)));
            Console.WriteLine(string.Format("Total Time Process SQL: {0} seconds", roundedSeconds));
        }

        static void ProcessRunCouchBaseServer(List<Product> products, string formatTime)
        {
            DateTime startTime = DateTime.Now;
            Console.WriteLine(string.Format("Start Time Couchbase: {0}", startTime.ToString(formatTime)));
            try
            {
                var cluster = Cluster.ConnectAsync("couchbase://127.0.0.1", "Admin", "123456aA@").GetAwaiter().GetResult();

                var bucket = cluster.BucketAsync("QUANLYTHONGTIN_COUCHBASE_NHOM4").GetAwaiter().GetResult();

                var collection = bucket.DefaultCollection();

                foreach (var product in products)
                {
                    var productId = Guid.NewGuid();
                    product.Id = productId;

                    foreach (var image in product.ProductImages)
                    {
                        image.Id = Guid.NewGuid();
                        image.ProductId = productId;
                    }

                    foreach (var detail in product.ProductDetails)
                    {
                        detail.Id = Guid.NewGuid();
                        detail.ProductId = productId;
                    }

                    collection.UpsertAsync(productId.ToString(), product).GetAwaiter().GetResult();
                }


                cluster.Dispose();
            }
            catch (Exception ex)
            {

            }

            DateTime endTime = DateTime.Now;
            var processSecond = endTime - startTime;
            double roundedSeconds = Math.Round(processSecond.TotalSeconds);
            Console.WriteLine(string.Format("End Time Couchbase: {0}", endTime.ToString(formatTime)));
            Console.WriteLine(string.Format("Total Time Process Couchbase: {0} seconds", roundedSeconds));
        }

        static void ProcessRunGetDataSQLDatabaseGetAll(string formatTime)
        {
            DateTime startTime = DateTime.Now;
            Console.WriteLine(string.Format("Start Time SQL: {0}", startTime.ToString(formatTime)));
            SqlConnection conn = new SqlConnection("Data Source=localhost\\SQLEXPRESS;Initial Catalog=QUANLYTHONGTIN_SQL_NHOM4;Persist Security Info=True;User ID=sa;Password=123456aA@");
            try
            {
                conn.Open();
                string query = @"
                DECLARE @jsonResult NVARCHAR(MAX);

                SET @jsonResult = 
                (
                    SELECT 
                        P.Id,
                        P.Name,
                        P.SKU,
                        P.Category,
                        P.Description,
                        P.CreatedBy,
                        P.UpdatedBy,
                        P.CreatedDate,
                        P.UpdatedDate,
                        
                        -- JSON Array cho ProductDetails
                        ISNULL(
                            (
                                SELECT 
                                    PD.Type,
                                    PD.Category,
                                    PD.Description,
                                    PD.CreatedBy,
                                    PD.UpdatedBy,
                                    PD.CreatedDate,
                                    PD.UpdatedDate
                                FROM ProductDetail PD
                                WHERE PD.ProductId = P.Id
                                FOR JSON PATH
                            ), '[]'
                        ) AS ProductDetails,
                        
                        -- JSON Array cho ProductImages
                        ISNULL(
                            (
                                SELECT 
                                    PI.Url,
                                    PI.CreatedBy,
                                    PI.UpdatedBy,
                                    PI.CreatedDate,
                                    PI.UpdatedDate
                                FROM ProductImage PI
                                WHERE PI.ProductId = P.Id
                                FOR JSON PATH
                            ), '[]'
                        ) AS ProductImages

                    FROM Product P
                    LEFT JOIN ProductDetail PD ON P.Id = PD.ProductId
                    LEFT JOIN ProductImage PI ON P.Id = PI.ProductId
                    GROUP BY P.Id, P.Name, P.SKU, P.Category, P.Description, 
                             P.CreatedBy, P.UpdatedBy, P.CreatedDate, P.UpdatedDate
                    FOR JSON PATH
                );

                SELECT @jsonResult AS FullJsonResult;
                ";

                SqlCommand command = new SqlCommand(query, conn)
                {
                    CommandTimeout = 300
                };

                SqlDataReader reader = command.ExecuteReader();
                if (reader.Read())
                {
                    string jsonResult = reader[0].ToString();
                    var products = JsonConvert.DeserializeObject<List<Product>>(jsonResult);
                    Console.WriteLine(string.Format("Total Records Founds: {0}", products.Count()));
                }

                conn.Close();

            }
            catch (Exception ex)
            {
                conn.Close();
                Console.WriteLine(ex.Message);
                Console.ReadKey();
            }
            DateTime endTime = DateTime.Now;
            var processSecond = endTime - startTime;
            double roundedSeconds = Math.Round(processSecond.TotalSeconds);
            Console.WriteLine(string.Format("End Time SQL: {0}", endTime.ToString(formatTime)));
            Console.WriteLine(string.Format("Total Time Process SQL: {0} seconds", roundedSeconds));
        }

        static async Task ProcessRunGetDataCouchBaseAsyncGetAll(string formatTime)
        {
            DateTime startTime = DateTime.Now;
            Console.WriteLine(string.Format("Start Time Couchbase: {0}", startTime.ToString(formatTime)));
            var cluster = Cluster.ConnectAsync("couchbase://127.0.0.1", "Admin", "123456aA@").GetAwaiter().GetResult();
            try
            {
                var bucket = await cluster.BucketAsync("QUANLYTHONGTIN_COUCHBASE_NHOM4");

                string query = @"SELECT * FROM `QUANLYTHONGTIN_COUCHBASE_NHOM4`";

                var collection = bucket.DefaultCollection();

                var result = await cluster.QueryAsync<Product>(query);

                if (result.MetaData.Status == QueryStatus.Success)
                {
                    var rowsList = await result.Rows.CountAsync();
                    Console.WriteLine(string.Format("Total Records Founds: {0}", rowsList));
                }

                await cluster.DisposeAsync();


            }
            catch (Exception ex)
            {
                await cluster.DisposeAsync();
                Console.WriteLine(ex.Message);
                Console.ReadKey();
            }
            DateTime endTime = DateTime.Now;
            var processSecond = endTime - startTime;
            double roundedSeconds = Math.Round(processSecond.TotalSeconds);
            Console.WriteLine(string.Format("End Time Couchbase: {0}", endTime.ToString(formatTime)));
            Console.WriteLine(string.Format("Total Time Process Couchbase: {0} seconds", roundedSeconds));
        }

        static void ProcessRunGetDataSQLDatabase(string formatTime)
        {
            DateTime startTime = DateTime.Now;
            Console.WriteLine(string.Format("Start Time SQL: {0}", startTime.ToString(formatTime)));
            SqlConnection conn = new SqlConnection("Data Source=localhost\\SQLEXPRESS;Initial Catalog=QUANLYTHONGTIN_SQL_NHOM4;Persist Security Info=True;User ID=sa;Password=123456aA@");
            try
            {
                conn.Open();
                string query = @"
                DECLARE @jsonResult NVARCHAR(MAX);

                SET @jsonResult = 
                (
                    SELECT TOP 10000
                        P.Id,
                        P.Name,
                        P.SKU,
                        P.Category,
                        P.Description,
                        P.CreatedBy,
                        P.UpdatedBy,
                        P.CreatedDate,
                        P.UpdatedDate,
                        
                        -- JSON Array cho ProductDetails
                        ISNULL(
                            (
                                SELECT 
                                    PD.Type,
                                    PD.Category,
                                    PD.Description,
                                    PD.CreatedBy,
                                    PD.UpdatedBy,
                                    PD.CreatedDate,
                                    PD.UpdatedDate
                                FROM ProductDetail PD
                                WHERE PD.ProductId = P.Id
                                FOR JSON PATH
                            ), '[]'
                        ) AS ProductDetails,
                        
                        -- JSON Array cho ProductImages
                        ISNULL(
                            (
                                SELECT 
                                    PI.Url,
                                    PI.CreatedBy,
                                    PI.UpdatedBy,
                                    PI.CreatedDate,
                                    PI.UpdatedDate
                                FROM ProductImage PI
                                WHERE PI.ProductId = P.Id
                                FOR JSON PATH
                            ), '[]'
                        ) AS ProductImages

                    FROM Product P
                    LEFT JOIN ProductDetail PD ON P.Id = PD.ProductId
                    LEFT JOIN ProductImage PI ON P.Id = PI.ProductId
                    WHERE PD.Category Like '%Washing%' 
                       OR PD.Type Like '%Techno%' 
                    GROUP BY P.Id, P.Name, P.SKU, P.Category, P.Description, 
                             P.CreatedBy, P.UpdatedBy, P.CreatedDate, P.UpdatedDate
                    FOR JSON PATH
                );

                SELECT @jsonResult AS FullJsonResult;
                ";

                SqlCommand command = new SqlCommand(query, conn)
                {
                    CommandTimeout = 300
                };

                SqlDataReader reader = command.ExecuteReader();
                if (reader.Read())
                {
                    string jsonResult = reader[0].ToString();
                    var products = JsonConvert.DeserializeObject<List<Product>>(jsonResult);
                    Console.WriteLine(string.Format("Total Records Founds: {0}", products.Count()));
                }

                conn.Close();

            }
            catch (Exception ex)
            {
                conn.Close();
                Console.WriteLine(ex.Message);
                Console.ReadKey();
            }
            DateTime endTime = DateTime.Now;
            var processSecond = endTime - startTime;
            double roundedSeconds = Math.Round(processSecond.TotalSeconds);
            Console.WriteLine(string.Format("End Time SQL: {0}", endTime.ToString(formatTime)));
            Console.WriteLine(string.Format("Total Time Process SQL: {0} seconds", roundedSeconds));
        }

        static async Task ProcessRunGetDataCouchBaseAsync(string formatTime)
        {
            DateTime startTime = DateTime.Now;
            Console.WriteLine(string.Format("Start Time Couchbase: {0}", startTime.ToString(formatTime)));
            var cluster = Cluster.ConnectAsync("couchbase://127.0.0.1", "Admin", "123456aA@").GetAwaiter().GetResult();
            try
            {
                var bucket = await cluster.BucketAsync("QUANLYTHONGTIN_COUCHBASE_NHOM4");

                string query = @"SELECT id, name, category, description, sku, productDetails, productImages
                                FROM `QUANLYTHONGTIN_COUCHBASE_NHOM4`
                                WHERE ANY pd IN productDetails SATISFIES 
                                      (LOWER(pd.category) LIKE '%washing%' OR LOWER(pd.type) LIKE '%techno%') 
                                      END
                                LIMIT 10000";


                var collection = bucket.DefaultCollection();

                var result = await cluster.QueryAsync<Product>(query);

                if (result.MetaData.Status == QueryStatus.Success)
                {
                    var rowsList = await result.Rows.CountAsync();
                    Console.WriteLine(string.Format("Total Records Founds: {0}", rowsList));
                }

                await cluster.DisposeAsync();


            }
            catch (Exception ex)
            {
                await cluster.DisposeAsync();
                Console.WriteLine(ex.Message);
                Console.ReadKey();
            }
            DateTime endTime = DateTime.Now;
            var processSecond = endTime - startTime;
            double roundedSeconds = Math.Round(processSecond.TotalSeconds);
            Console.WriteLine(string.Format("End Time Couchbase: {0}", endTime.ToString(formatTime)));
            Console.WriteLine(string.Format("Total Time Process Couchbase: {0} seconds", roundedSeconds));
        }
    }
}
