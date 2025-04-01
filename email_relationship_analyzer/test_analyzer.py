import json
import logging
import os
import sys

# 获取当前文件所在目录
current_dir = os.path.dirname(os.path.abspath(__file__))

from email_analyzer import EmailRelationshipAnalyzer
from test_data import generate_test_data, create_test_chunks
from config import DEFAULT_OUTPUT_FILE, DEFAULT_ERROR_FILE, LOG_FORMAT

# 设置日志
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

class TestEmailRelationshipAnalyzer(EmailRelationshipAnalyzer):
    """用于测试的邮件关系分析器子类"""
    
    def __init__(self):
        """初始化测试分析器，不需要文件路径"""
        # 不调用父类初始化方法，而是直接设置属性
        self.excel_file_path = "测试数据"
        self.relationships = {}
        self.unknown_data = {
            "invalid_emails": [],      # 无效的邮箱格式
            "empty_data": [],          # 空数据
            "multiple_senders": {},    # 相同主题不同发件人
            "processing_errors": [],   # 处理过程中的错误
            "unknown_languages": []    # 未知语言的邮件
        }
        self.required_columns = ['邮件名称', '发件人', '收件人', '邮件消息标识']
        self.subject_sender_map = {}  # 用于跟踪相同主题的不同发件人
        self.pending_replies = {}  # 用于暂存找不到原始邮件的回复
        self.original_emails = {}  # 用于存储所有原始邮件
    
    def run_analysis_on_test_data(self):
        """在测试数据上运行分析"""
        logger.info("开始分析测试数据")
        
        # 生成测试数据
        test_data = generate_test_data()
        logger.info(f"生成了 {len(test_data)} 条测试数据")
        
        # 分块处理测试数据
        chunks = create_test_chunks(test_data, chunk_size=3)
        logger.info(f"将测试数据分成 {len(chunks)} 个块")
        
        # 处理每个数据块
        for i, chunk in enumerate(chunks):
            logger.info(f"处理数据块 {i+1}/{len(chunks)}")
            try:
                self.process_chunk(chunk)
            except Exception as e:
                logger.error(f"处理数据块时发生错误: {str(e)}")
                self.unknown_data["processing_errors"].append({
                    "chunk_hash": hash(tuple(chunk.iloc[0])),
                    "error": str(e),
                    "type": "chunk_processing_error"
                })
        
        # 处理暂存的回复邮件
        self.process_pending_replies()
        
        # 检查是否找到了关系
        if not self.relationships:
            logger.warning("未找到任何关系集合")
        else:
            total_errors = (
                len(self.unknown_data["invalid_emails"]) +
                len(self.unknown_data["empty_data"]) +
                len(self.unknown_data["multiple_senders"]) +
                len(self.unknown_data["processing_errors"]) +
                len(self.unknown_data["unknown_languages"])
            )
            logger.info(f"分析完成，找到 {len(self.relationships)} 个关系集合，记录 {total_errors} 个异常")
        
        return self.relationships, self.unknown_data

def main():
    """运行测试"""
    try:
        # 初始化测试分析器
        analyzer = TestEmailRelationshipAnalyzer()
        
        # 运行分析
        relationships, unknown_data = analyzer.run_analysis_on_test_data()
        
        # 保存结果
        analyzer.save_relationships(DEFAULT_OUTPUT_FILE)
        analyzer.save_unknown_data(DEFAULT_ERROR_FILE)
        
        # 显示结果
        print("\n=== 找到的关系集合 ===")
        for key, values in relationships.items():
            print(f"\n{key}:")
            for value in values:
                print(f"  - {value}")
        
        print("\n=== 记录的异常数据 ===")
        for category, items in unknown_data.items():
            if items:  # 只显示非空的类别
                print(f"\n{category}:")
                if isinstance(items, dict):
                    for key, value in items.items():
                        print(f"  {key}: {value}")
                else:
                    for item in items:
                        print(f"  - {item}")
        
        logger.info("测试完成!")
        
    except Exception as e:
        logger.error(f"测试过程中发生错误: {str(e)}")
        # 如果分析器已经初始化，尝试保存已收集的异常数据
        if 'analyzer' in locals():
            try:
                analyzer.save_unknown_data(DEFAULT_ERROR_FILE)
            except Exception as save_error:
                logger.error(f"保存异常数据时出错: {str(save_error)}")

if __name__ == "__main__":
    main() 