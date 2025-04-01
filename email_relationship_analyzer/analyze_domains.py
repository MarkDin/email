import pandas as pd
import json
import logging
from pathlib import Path
from typing import Dict, List, Set

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DomainAnalyzer:
    def __init__(self, excel_path: str, output_path: str = 'domain.json'):
        """
        初始化域名分析器
        
        Args:
            excel_path: Excel文件路径
            output_path: 输出JSON文件路径
        """
        self.excel_path = excel_path
        self.output_path = output_path
        self.multi_domain_messages: Dict[str, List[str]] = {}

    def extract_domains(self, recipients: str) -> Set[str]:
        """
        从收件人字符串中提取所有域名
        
        Args:
            recipients: 收件人字符串，可能包含多个邮箱地址
            
        Returns:
            包含所有不同域名的集合
        """
        if not isinstance(recipients, str):
            return set()
            
        domains = set()
        # 分割多个收件人
        for email in recipients.split(','):
            email = email.strip()
            if '@' in email:
                try:
                    domain = email.split('@')[1].strip()
                    if domain:
                        domains.add(domain)
                except Exception as e:
                    logger.warning(f"处理邮箱 {email} 时出错: {str(e)}")
        return domains

    def analyze(self):
        """分析Excel文件中的域名情况"""
        try:
            logger.info(f"开始读取Excel文件: {self.excel_path}")
            df = pd.read_excel(self.excel_path)
            
            if '收件人' not in df.columns or '邮件消息标识' not in df.columns:
                raise ValueError("Excel文件必须包含'收件人'和'邮件消息标识'列")

            total_rows = len(df)
            processed_rows = 0
            
            for _, row in df.iterrows():
                message_id = row['邮件消息标识']
                recipients = row['收件人']
                
                if pd.isna(message_id) or pd.isna(recipients):
                    continue
                    
                domains = self.extract_domains(str(recipients))
                if len(domains) > 2:
                    self.multi_domain_messages[str(message_id)] = list(domains)
                
                processed_rows += 1
                if processed_rows % 1000 == 0:
                    logger.info(f"已处理 {processed_rows}/{total_rows} 行")

            self.save_results()
            logger.info(f"分析完成。发现 {len(self.multi_domain_messages)} 条包含多个域名的记录")
            
        except Exception as e:
            logger.error(f"分析过程中出错: {str(e)}")
            raise

    def save_results(self):
        """保存分析结果到JSON文件"""
        try:
            with open(self.output_path, 'w', encoding='utf-8') as f:
                json.dump(self.multi_domain_messages, f, ensure_ascii=False, indent=2)
            logger.info(f"结果已保存到: {self.output_path}")
        except Exception as e:
            logger.error(f"保存结果时出错: {str(e)}")
            raise

def main():
    # 设置Excel文件路径
    excel_path = "/Users/dingke/Downloads/emails.xlsx"  # 请根据实际情况修改文件路径
    output_path = "domain.json"
    
    try:
        analyzer = DomainAnalyzer(excel_path, output_path)
        analyzer.analyze()
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
        raise

if __name__ == "__main__":
    main() 