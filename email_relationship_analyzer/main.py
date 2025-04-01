import logging
from config import DEFAULT_OUTPUT_FILE, DEFAULT_ERROR_FILE, LOG_FORMAT
from email_analyzer import EmailRelationshipAnalyzer

# 设置日志
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

def main():
    # 设置输入和输出文件路径
    input_file = "/Users/dingke/Downloads/emails.xlsx"  # 请替换为实际的Excel文件路径
    output_file = DEFAULT_OUTPUT_FILE
    error_file = DEFAULT_ERROR_FILE
    
    try:
        # 初始化分析器
        analyzer = EmailRelationshipAnalyzer(input_file)
        
        # 执行分析
        analyzer.analyze()
        
        # 保存结果
        analyzer.save_relationships(output_file)
        analyzer.save_errors(error_file)
        
        logger.info("处理完成!")
        
    except Exception as e:
        logger.error(f"处理过程中发生错误: {str(e)}")
        # 如果分析器已经初始化，尝试保存已收集的错误
        if 'analyzer' in locals():
            analyzer.save_errors(error_file)

if __name__ == "__main__":
    main() 