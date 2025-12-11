import os
import shutil
import time
import glob
from datetime import datetime
import logging

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataSimulator:
    """
    Giả lập việc nhận dữ liệu từ N cửa hàng theo thời gian thực
    """
    
    def __init__(self, 
                 source_dir='/home/hduser/data',
                 dest_dir='/home/hduser/realtime-data',
                 num_shops=60,
                 interval_seconds=3600):  # Default: 1 giờ = 3600s
        """
        Args:
            source_dir: Thư mục chứa dữ liệu gốc
            dest_dir: Thư mục đích (realtime data)
            num_shops: Số lượng cửa hàng (N)
            interval_seconds: Khoảng thời gian giữa các lần copy (T giây)
        """
        self.source_dir = source_dir
        self.dest_dir = dest_dir
        self.num_shops = num_shops
        self.interval_seconds = interval_seconds
        self.current_index = 0
        
        # Tạo thư mục đích nếu chưa có
        os.makedirs(dest_dir, exist_ok=True)
        
        # Lấy danh sách tất cả files và sắp xếp theo thời gian
        self.all_files = self._get_sorted_files()
        self.total_batches = len(self.all_files) // num_shops
        
        logger.info(f"Initialized DataSimulator:")
        logger.info(f"  - Source: {source_dir}")
        logger.info(f"  - Destination: {dest_dir}")
        logger.info(f"  - Shops: {num_shops}")
        logger.info(f"  - Interval: {interval_seconds}s ({interval_seconds/3600}h)")
        logger.info(f"  - Total files: {len(self.all_files)}")
        logger.info(f"  - Total batches: {self.total_batches}")
    
    def _get_sorted_files(self):
        """
        Lấy danh sách files và sắp xếp theo thời gian
        Returns: List of tuples (timestamp, filepath)
        """
        pattern = os.path.join(self.source_dir, 'Shop-*-*.csv')
        files = glob.glob(pattern)
        
        # Parse timestamp từ tên file: Shop-k-YYYYMMDD-hh.csv
        file_timestamps = []
        for filepath in files:
            filename = os.path.basename(filepath)
            try:
                # Extract: Shop-1-20210104-06.csv -> 20210104-06
                parts = filename.replace('.csv', '').split('-')
                shop_id = int(parts[1])
                date_str = parts[2]
                hour_str = parts[3]
                
                # Create timestamp
                timestamp = datetime.strptime(f"{date_str}-{hour_str}", "%Y%m%d-%H")
                file_timestamps.append((timestamp, shop_id, filepath))
            except (ValueError, IndexError) as e:
                logger.warning(f"Skipping invalid file: {filename} - {e}")
                continue
        
        # Sắp xếp theo timestamp, sau đó theo shop_id
        file_timestamps.sort(key=lambda x: (x[0], x[1]))
        
        return [(ts, fp) for ts, sid, fp in file_timestamps]
    
    def _get_next_batch(self):
        """
        Lấy N files tiếp theo (1 file cho mỗi shop)
        Returns: List of filepaths
        """
        start_idx = self.current_index
        end_idx = start_idx + self.num_shops
        
        if end_idx > len(self.all_files):
            logger.warning("Reached end of data. Restarting from beginning.")
            self.current_index = 0
            start_idx = 0
            end_idx = self.num_shops
        
        batch = [filepath for _, filepath in self.all_files[start_idx:end_idx]]
        self.current_index = end_idx
        
        return batch
    
    def copy_batch(self):
        """
        Copy một batch files từ source sang destination
        """
        batch_files = self._get_next_batch()
        
        if not batch_files:
            logger.error("No files to copy!")
            return False
        
        # Lấy timestamp từ file đầu tiên để log
        first_file = os.path.basename(batch_files[0])
        timestamp_info = first_file.replace('.csv', '').split('-')[2:4]
        
        logger.info(f"Copying batch {self.current_index // self.num_shops}: "
                   f"Date={timestamp_info[0]}, Hour={timestamp_info[1]}")
        
        success_count = 0
        for source_path in batch_files:
            try:
                filename = os.path.basename(source_path)
                dest_path = os.path.join(self.dest_dir, filename)
                
                # Copy file
                shutil.copy2(source_path, dest_path)
                success_count += 1
                logger.debug(f"  Copied: {filename}")
                
            except Exception as e:
                logger.error(f"  Failed to copy {filename}: {e}")
        
        logger.info(f"Batch completed: {success_count}/{len(batch_files)} files copied")
        return success_count == len(batch_files)
    
    def run_once(self):
        """
        Chạy một lần copy batch
        """
        logger.info("=" * 60)
        logger.info(f"Starting batch copy at {datetime.now()}")
        success = self.copy_batch()
        logger.info(f"Batch copy {'succeeded' if success else 'failed'}")
        logger.info("=" * 60)
        return success
    
    def run_continuous(self):
        """
        Chạy liên tục với interval T
        """
        logger.info("Starting continuous simulation...")
        logger.info(f"Will copy {self.num_shops} files every {self.interval_seconds}s")
        logger.info("Press Ctrl+C to stop")
        
        try:
            batch_number = 0
            while True:
                batch_number += 1
                logger.info(f"\n{'='*60}")
                logger.info(f"BATCH #{batch_number} - {datetime.now()}")
                logger.info(f"{'='*60}")
                
                self.copy_batch()

                logger.info(f"Waiting {self.interval_seconds}s until next batch...")
                time.sleep(self.interval_seconds)
                
        except KeyboardInterrupt:
            logger.info("\nSimulation stopped by user")
        except Exception as e:
            logger.error(f"Simulation error: {e}")
            raise

def main():
    """
    Main function
    """
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Simulate realtime data streaming from shops'
    )
    parser.add_argument(
        '--source',
        default='/home/hduser/data',
        help='Source directory containing all data files'
    )
    parser.add_argument(
        '--dest',
        default='/home/hduser/realtime-data',
        help='Destination directory for realtime data'
    )
    parser.add_argument(
        '--shops',
        type=int,
        default=60,
        help='Number of shops (N)'
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=3600,
        help='Interval in seconds between batches (T)'
    )
    parser.add_argument(
        '--once',
        action='store_true',
        help='Run only once (for testing)'
    )
    
    args = parser.parse_args()
    
    # Khởi tạo simulator
    simulator = DataSimulator(
        source_dir=args.source,
        dest_dir=args.dest,
        num_shops=args.shops,
        interval_seconds=args.interval
    )
    
    # Chạy
    if args.once:
        simulator.run_once()
    else:
        simulator.run_continuous()

if __name__ == '__main__':
    main()
