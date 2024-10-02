using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QUANLYTHONGTIN_SOURCE.Models
{
    public class Product
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
        public string SKU { get; set; }
        public string Category { get; set; }
        public string Description { get; set; }
        public List<ProductImage> ProductImages { get; set; }
        public List<ProductDetail> ProductDetails { get; set; }
        public bool Delete { get; set; }
        public string CreatedBy { get; set; }
        public string UpdatedBy { get; set; }
        public DateTime CreatedDate { get; set; }
        public DateTime UpdatedDate { get; set; }
    }
}
