


import json
import pandas as pd
import logging

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_field_from_excel(file_path, field_name):
    try:
        # 读取Excel文件某个字段的全部值
        df = pd.read_excel(file_path)
        
        grouped_data = set()
        # 确保字段名存在于DataFrame中
        if field_name not in df.columns:
            logger.error(f"字段名 '{field_name}' 不存在于Excel文件中")
            return grouped_data
        
        # 遍历DataFrame中的每一行，提取指定字段的值
        for _, row in df.iterrows():
            value = row[field_name]
            # 确保值不为空
            if pd.notna(value):
                # 如果值是数值类型，转换为字符串
                if isinstance(value, (int, float)):
                    value = str(int(value))
                else:
                    value = str(value)
                # 将值添加到集合中
                grouped_data.add(value)
        
        logger.info(f"从Excel文件中读取了 {len(grouped_data)} 个唯一的 {field_name} 值")
        return grouped_data
    except Exception as e:
        logger.error(f"加载relationships.json时出错: {str(e)}")
        raise


def check_not_exist(source_data: set, target_data: set):
    """检查source_data中是否存在target_data中不存在的值"""
    not_exist = source_data - target_data
    return not_exist



def main():
    source_data = load_field_from_excel('/Users/dingke/Downloads/emails.xlsx', '邮件消息标识')
    target_data = load_field_from_excel('/Users/dingke/Library/Containers/com.tencent.xinWeChat/Data/Library/Application Support/com.tencent.xinWeChat/2.0b4.0.9/ef83a504a301948b449baac80d05a819/Message/MessageTemp/447b05e0a8d37b6b54692bb6a7fbf7af/File/xiaomei.xlsx', 'email_message_tag')
    not_exist = check_not_exist(source_data, target_data)
    # 将不存在的数据写入JSON文件
    try:
        with open('not_exist.json', 'w', encoding='utf-8') as f:
            # 将集合转换为列表，以便JSON序列化
            json.dump(list(not_exist), f, ensure_ascii=False, indent=2)
        logger.info(f"不存在的数据已保存到: not_exist.json")
    except Exception as e:
        logger.error(f"保存不存在数据时出错: {str(e)}")
    print(len(not_exist))

main()