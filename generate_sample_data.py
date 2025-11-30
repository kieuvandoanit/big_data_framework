import os
import csv
import random
from datetime import datetime, timedelta
import logging

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataGenerator:
    """
    Tạo dữ liệu giả lập cho hệ thống bán hàng
    """
    
    # Danh sách sản phẩm mẫu (100 sản phẩm)
    PRODUCTS = [
        ("Cà phê Đen", 25000, 35000),
        ("Cà phê Sữa", 29000, 39000),
        ("Espresso", 45000, 59000),
        ("Cappuccino", 45000, 55000),
        ("Latte", 45000, 55000),
        ("Mocha", 49000, 59000),
        ("Americano", 39000, 49000),
        ("Macchiato", 45000, 55000),
        ("Trà Sữa Truyền Thống", 35000, 45000),
        ("Trà Sữa Matcha", 39000, 49000),
        ("Trà Sữa Socola", 39000, 49000),
        ("Trà Sữa Dâu", 39000, 49000),
        ("Trà Đào Cam Sả", 35000, 45000),
        ("Trà Chanh", 25000, 35000),
        ("Trà Vải", 29000, 39000),
        ("Sinh Tố Bơ", 39000, 49000),
        ("Sinh Tố Xoài", 35000, 45000),
        ("Sinh Tố Dâu", 35000, 45000),
        ("Sinh Tố Sapoche", 35000, 45000),
        ("Nước Ép Cam", 29000, 39000),
        ("Nước Ép Dưa Hấu", 25000, 35000),
        ("Nước Ép Táo", 29000, 39000),
        ("Bánh Mì Thịt", 25000, 35000),
        ("Bánh Mì Trứng", 20000, 30000),
        ("Bánh Mì Pate", 20000, 30000),
        ("Bánh Ngọt Socola", 35000, 45000),
        ("Bánh Ngọt Kem", 35000, 45000),
        ("Bánh Croissant", 29000, 39000),
        ("Sandwich", 35000, 45000),
        ("Hamburger", 45000, 55000),
        ("Pizza Mini", 49000, 69000),
        ("Mì Ý", 55000, 75000),
        ("Cơm Rang", 35000, 45000),
        ("Xúc Xích", 25000, 35000),
        ("Gà Rán", 35000, 49000),
        ("Snack Khoai Tây", 15000, 25000),
        ("Snack Bắp Rang", 12000, 20000),
        ("Kẹo Socola", 10000, 20000),
        ("Kẹo Dẻo", 8000, 15000),
        ("Bánh Quy", 15000, 25000),
    ]
    
    def __init__(self, 
                 output_dir='/home/hduser/data',
                 num_shops=60,
                 start_date='2021-01-01',
                 days=270):  # 270 ngày = ~9 tháng
        """
        Args:
            output_dir: Thư mục lưu dữ liệu
            num_shops: Số lượng cửa hàng
            start_date: Ngày bắt đầu (YYYY-MM-DD)
            days: Số ngày tạo dữ liệu
        """
        self.output_dir = output_dir
        self.num_shops = num_shops
        self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        self.days = days
        self.total_files = num_shops * 24 * days
        
        # Tạo thư mục output
        os.makedirs(output_dir, exist_ok=True)
        
        # Tạo danh sách sản phẩm với ID
        self.products = []
        for idx, (name, min_price, max_price) in enumerate(self.PRODUCTS, start=101):
            self.products.append({
                'id': idx,
                'name': name,
                'min_price': min_price,
                'max_price': max_price
            })
        
        logger.info(f"DataGenerator initialized:")
        logger.info(f"  - Output: {output_dir}")
        logger.info(f"  - Shops: {num_shops}")
        logger.info(f"  - Start date: {start_date}")
        logger.info(f"  - Days: {days}")
        logger.info(f"  - Total files: {self.total_files:,}")
        logger.info(f"  - Products: {len(self.products)}")
    
    def _generate_orders(self, shop_id, date, hour):
        """
        Tạo danh sách orders cho một file
        
        Args:
            shop_id: ID cửa hàng
            date: Ngày
            hour: Giờ
            
        Returns:
            List of orders
        """
        # Số lượng đơn hàng ngẫu nhiên (5-30 đơn/giờ)
        # Giờ cao điểm (7-9h, 11-13h, 17-19h) có nhiều đơn hơn
        if hour in [7, 8, 11, 12, 17, 18]:
            num_orders = random.randint(15, 30)
        elif hour in [6, 9, 10, 13, 14, 16, 19, 20]:
            num_orders = random.randint(10, 20)
        else:
            num_orders = random.randint(5, 15)
        
        orders = []
        base_order_id = int(f"{date.strftime('%Y%m%d')}{hour:02d}{shop_id:03d}000")
        
        for i in range(num_orders):
            order_id = base_order_id + i
            
            # Số lượng items trong đơn hàng (1-5 items)
            num_items = random.randint(1, 5)
            
            # Chọn ngẫu nhiên sản phẩm
            selected_products = random.sample(self.products, num_items)
            
            for product in selected_products:
                # Số lượng mỗi sản phẩm (1-3)
                amount = random.randint(1, 3)
                
                # Giá ngẫu nhiên trong khoảng
                price = random.randint(
                    product['min_price'], 
                    product['max_price']
                )
                
                # Discount ngẫu nhiên (0%, 5%, 10%, 15%)
                discount = random.choice([0, 0, 0, 5, 10, 15])  # 0% có xác suất cao hơn
                
                orders.append({
                    'OrderID': order_id,
                    'ProductID': product['id'],
                    'ProductName': product['name'],
                    'Amount': amount,
                    'Price': price,
                    'Discount': discount
                })
        
        return orders
    
    def _write_csv_file(self, filepath, orders):
        """
        Ghi dữ liệu vào file CSV
        
        Args:
            filepath: Đường dẫn file
            orders: Danh sách orders
        """
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'OrderID', 'ProductID', 'ProductName', 'Amount', 'Price', 'Discount'
            ])
            writer.writeheader()
            writer.writerows(orders)
    
    def generate_all(self):
        """
        Tạo tất cả files dữ liệu
        """
        logger.info("Starting data generation...")
        logger.info("This may take a while...")
        
        files_created = 0
        start_time = datetime.now()
        
        try:
            for day in range(self.days):
                current_date = self.start_date + timedelta(days=day)
                
                for hour in range(24):
                    for shop_id in range(1, self.num_shops + 1):
                        # Tên file: Shop-k-YYYYMMDD-hh.csv
                        filename = f"Shop-{shop_id}-{current_date.strftime('%Y%m%d')}-{hour:02d}.csv"
                        filepath = os.path.join(self.output_dir, filename)
                        
                        # Tạo orders
                        orders = self._generate_orders(shop_id, current_date, hour)
                        
                        # Ghi file
                        self._write_csv_file(filepath, orders)
                        
                        files_created += 1
                        
                        # Log progress mỗi 1000 files
                        if files_created % 1000 == 0:
                            elapsed = (datetime.now() - start_time).total_seconds()
                            rate = files_created / elapsed if elapsed > 0 else 0
                            eta = (self.total_files - files_created) / rate if rate > 0 else 0
                            
                            logger.info(
                                f"Progress: {files_created:,}/{self.total_files:,} "
                                f"({files_created/self.total_files*100:.1f}%) - "
                                f"Rate: {rate:.1f} files/s - "
                                f"ETA: {eta/60:.1f} min"
                            )
            
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info("=" * 60)
            logger.info(f"Data generation completed!")
            logger.info(f"  - Files created: {files_created:,}")
            logger.info(f"  - Time elapsed: {elapsed/60:.2f} minutes")
            logger.info(f"  - Average rate: {files_created/elapsed:.1f} files/s")
            logger.info(f"  - Output directory: {self.output_dir}")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"Error during generation: {e}")
            raise
    
    def generate_sample(self, num_files=100):
        """
        Tạo một số lượng nhỏ files để test (100 files = ~1.67 giờ)
        
        Args:
            num_files: Số lượng files cần tạo
        """
        logger.info(f"Generating {num_files} sample files for testing...")
        
        files_created = 0
        current_date = self.start_date
        hour = 0
        shop_id = 1
        
        while files_created < num_files:
            filename = f"Shop-{shop_id}-{current_date.strftime('%Y%m%d')}-{hour:02d}.csv"
            filepath = os.path.join(self.output_dir, filename)
            
            orders = self._generate_orders(shop_id, current_date, hour)
            self._write_csv_file(filepath, orders)
            
            files_created += 1
            
            # Tăng shop_id
            shop_id += 1
            if shop_id > self.num_shops:
                shop_id = 1
                hour += 1
                if hour >= 24:
                    hour = 0
                    current_date += timedelta(days=1)
            
            if files_created % 10 == 0:
                logger.info(f"Created {files_created}/{num_files} files...")
        
        logger.info(f"Sample generation completed! Files in: {self.output_dir}")

def main():
    """
    Main function
    """
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Generate sample shop sales data'
    )
    parser.add_argument(
        '--output',
        default='/home/hduser/data',
        help='Output directory'
    )
    parser.add_argument(
        '--shops',
        type=int,
        default=60,
        help='Number of shops'
    )
    parser.add_argument(
        '--start-date',
        default='2021-01-01',
        help='Start date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=270,
        help='Number of days to generate'
    )
    parser.add_argument(
        '--sample',
        type=int,
        default=0,
        help='Generate only N sample files (for testing). 0 = generate all'
    )
    
    args = parser.parse_args()
    
    # Khởi tạo generator
    generator = DataGenerator(
        output_dir=args.output,
        num_shops=args.shops,
        start_date=args.start_date,
        days=args.days
    )
    
    # Tạo dữ liệu
    if args.sample > 0:
        generator.generate_sample(args.sample)
    else:
        generator.generate_all()

if __name__ == '__main__':
    main()
