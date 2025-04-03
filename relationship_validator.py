import json
import logging
from excel_searcher import ExcelSearcher
from typing import Dict, List, Any
from tqdm import tqdm

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RelationshipValidator:
    def __init__(self, excel_file: str, relationships_file: str):
        """初始化验证器
        
        Args:
            excel_file (str): Excel文件路径
            relationships_file (str): relationships.json文件路径
        """
        self.excel_searcher = ExcelSearcher(excel_file)
        self.relationships_file = relationships_file
        self.bad_cases = []
        
    def load_relationships(self) -> Dict[str, Any]:
        """加载relationships数据
        
        Returns:
            Dict: relationships数据
        """
        with open(self.relationships_file, 'r', encoding='utf-8') as f:
            return json.load(f)
            
    def validate_relationship_item(self, item: List[str], search_results: List[Dict[str, Any]]) -> bool:
        """验证单个relationship item是否符合规则
        
        Args:
            item (List[str]): relationship item [sender, subject, recipient, message_id]
            search_results (List[Dict]): 搜索结果
            
        Returns:
            bool: 是否符合规则
        """
        sender, subject, recipient, message_id = item
        
        # 在搜索结果中查找匹配的行
        for result in search_results:
            data = result['data']
            
            # 检查发件人
            if sender not in data['发件人'] and sender not in data['收件人']:
                continue
                
            # 检查邮件名称
            if subject not in data['邮件名称']:
                continue
                
            # 检查收件人
            if recipient not in data['收件人'] and recipient not in data['发件人']:
                continue
                
            # 所有规则都满足
            return True
            
        # 没有找到匹配的行
        return False
        
    def validate(self, key: str, items: List[List[str]]):
        """验证relationships中的单个item"""
        logger.info("开始验证relationships...")
        fail_count = 0
        if len(items) == 0:
            logger.warning("item为空，跳过")
            return
        message_ids = set()
        row_ids = set()
        for item in items:
            message_id = item[3]
            if message_id.startswith('ROW_'):
                row_ids.add(message_id)
            else:
                message_ids.add(message_id)
                
        # 将message_ids和row_ids转换为列表
        message_ids = list(message_ids)
        row_ids = list(row_ids)
        
        # 使用ExcelSearcher搜索所有message_ids
        logger.info(f"开始搜索 {len(message_ids)} 个邮件消息标识和 {len(row_ids)} 个行号标识...")
        
        # 分别搜索message_ids和row_ids
        search_results = []
        if message_ids:
            message_results = self.excel_searcher.global_search(message_ids)
            search_results.extend(message_results)
            
        if row_ids:
            # 对于行号标识，我们需要检查每个行
            for row_id in row_ids:
                row_num = int(row_id.replace('ROW_', ''))
                # 由于Excel是从1开始计数，而DataFrame是从0开始，所以需要调整行号
                if 0 <= row_num < len(self.excel_searcher.df):
                    row_data = self.excel_searcher.df.iloc[row_num].to_dict()
                    search_results.append({
                        'row_index': row_num + 2,  # 加2是因为Excel有标题行，且从1开始计数
                        'data': row_data
                    })
        
        # 创建搜索结果的索引，以便快速查找
        search_results_index = {}
        for result in search_results:
            # 对于使用行号的情况，我们需要特殊处理
            if 'ROW_' + str(result['row_index'] - 2) in row_ids:
                search_results_index['ROW_' + str(result['row_index'] - 2)] = result
            else:
                search_results_index[result['data']['邮件消息标识']] = result
        
        # 验证
        for item in items:
            message_id = item[3]
            
            # 查找对应的搜索结果
            if message_id not in search_results_index:
                # 没有找到对应的邮件
                self.bad_cases.append({
                    'key': key,
                    'item': item,
                    'error': '未找到对应的邮件消息标识或行号',
                    'message_id': message_id
                })
                fail_count += 1
                continue
                
            # 验证规则
            search_result = [search_results_index[message_id]]
            if not self.validate_relationship_item(item, search_result):
                # 规则验证失败
                self.bad_cases.append({
                    'key': key,
                    'item': item,
                    'error': '规则验证失败',
                    'found_data': search_results_index[message_id]['data']
                })
                fail_count += 1
        
        logger.info(f"本轮验证完成，发现 {fail_count} 个问题案例")
        
    def save_bad_cases(self):
        """保存验证失败的案例到badcase.json"""
        with open('badcase.json', 'w', encoding='utf-8') as f:
            json.dump(self.bad_cases, f, ensure_ascii=False, indent=2)
        logger.info("问题案例已保存到 badcase.json")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='验证relationships结果')
    parser.add_argument('excel_file', help='Excel文件路径')
    parser.add_argument('relationships_file', help='relationships.json文件路径')
    parser.add_argument('limit', help='限制验证的item数量')
    
    args = parser.parse_args()
    limit = int(args.limit)
    cnt = 0
    validator = RelationshipValidator(args.excel_file, args.relationships_file)
    relationships = validator.load_relationships()
    for key, value in relationships.items():
        cnt += 1
        if cnt > limit:
            break
        validator.validate(key, value['items'])
    # 保存验证结果
    validator.save_bad_cases()
        
    logger.info(f"验证完成，发现 {len(validator.bad_cases)} 个问题案例")
if __name__ == '__main__':
    main() 