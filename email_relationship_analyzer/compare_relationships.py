import json
import pandas as pd
import logging
from collections import defaultdict

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_relationships(json_file):
    """加载relationships.json文件"""
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"加载relationships.json时出错: {str(e)}")
        raise

def process_excel_data(excel_file):
    """处理Excel文件，按topic_group_id分组并生成唯一标识"""
    try:
        # 读取Excel文件
        df = pd.read_excel(excel_file)
        
        # 按topic_group_id分组
        grouped_data = {}
        for _, row in df.iterrows():
            topic_group_id = row['topic_group_id']
            if pd.notna(topic_group_id):  # 确保topic_group_id不为空
                # 将topic_group_id转换为整数，再转换为字符串
                topic_group_id = str(int(topic_group_id))
                if topic_group_id not in grouped_data:
                    grouped_data[topic_group_id] = []
                # 确保email_message_tag是字符串类型
                email_tag = str(row['email_message_tag'])
                grouped_data[topic_group_id].append(email_tag)
        
        # 对每个组的email_message_tag进行排序并拼接
        for topic_group_id in grouped_data:
            # if len(grouped_data[topic_group_id]) > 4:
            #     continue
            grouped_data[topic_group_id].sort()
            # grouped_data[topic_group_id] = '#'.join(grouped_data[topic_group_id])
            key = '#'.join(grouped_data[topic_group_id])
            grouped_data[key] = 1
            # print(grouped_data[topic_group_id])
        
        return grouped_data
    except Exception as e:
        logger.error(f"处理Excel文件时出错: {str(e)}")
        raise

def process_json_data(json_data):
    """处理JSON数据，生成唯一标识"""
    processed_data = {}
    for key, value in json_data.items():
        # if value['count'] > 4:
        #     continue
        # 从items中提取email_message_tag
        message_tags = [item[3] for item in value['items']]
        # 排序并拼接
        message_tags.sort()
        new_key = '#'.join(message_tags)
        processed_data[new_key] = 1
    return processed_data

def compare_relationships(excel_data, json_data):
    """比较两个数据源的关系集合"""
    mismatches = {
        "missing_in_excel": [],  # 在Excel中找不到的关系
        "missing_in_json": [],   # 在JSON中找不到的关系
        "different_sequences": []  # 邮件顺序不一致的关系
    }
    
    # 处理JSON数据
    processed_json = process_json_data(json_data)
    
    # 比较每个topic_group_id
    for topic_group_id, excel_sequence in excel_data.items():
        # 检查是否在JSON中存在
        if topic_group_id not in processed_json:
            mismatches["missing_in_json"].append({
                "topic_group_id": topic_group_id,
                "excel_sequence": excel_sequence
            })
            continue
        
        # 比较序列是否一致
        if excel_sequence != processed_json[topic_group_id]:
            mismatches["different_sequences"].append({
                "topic_group_id": topic_group_id,
                "excel_sequence": excel_sequence,
                "json_sequence": processed_json[topic_group_id]
            })
    
    # 检查JSON中是否有Excel中找不到的关系
    for topic_group_id, json_sequence in processed_json.items():
        if topic_group_id not in excel_data:
            mismatches["missing_in_excel"].append({
                "topic_group_id": topic_group_id,
                "json_sequence": json_sequence
            })
    
    return mismatches

def save_mismatches(mismatches, output_file):
    """保存不匹配的结果到JSON文件"""
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(mismatches, f, ensure_ascii=False, indent=2)
        logger.info(f"不匹配的结果已保存到: {output_file}")
    except Exception as e:
        logger.error(f"保存不匹配结果时出错: {str(e)}")
        raise

def main():
    """主函数"""
    try:
        # 加载数据
        logger.info("开始加载数据...")
        json_data = load_relationships('relationships.json')
        excel_data = process_excel_data('/Users/dingke/Library/Containers/com.tencent.xinWeChat/Data/Library/Application Support/com.tencent.xinWeChat/2.0b4.0.9/ef83a504a301948b449baac80d05a819/Message/MessageTemp/447b05e0a8d37b6b54692bb6a7fbf7af/File/xiaomei.xlsx')
        
        # 比较关系集合
        logger.info("开始比较关系集合...")
        mismatches = compare_relationships(excel_data, json_data)
        
        # 输出统计信息
        logger.info("比较结果统计:")
        logger.info(f"- Excel中找不到的关系: {len(mismatches['missing_in_excel'])}")
        logger.info(f"- JSON中找不到的关系: {len(mismatches['missing_in_json'])}")
        logger.info(f"- 邮件顺序不一致的关系: {len(mismatches['different_sequences'])}")
        
        # 保存不匹配的结果
        save_mismatches(mismatches, 'mismatches.json')
        
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
        raise

if __name__ == "__main__":
    main() 