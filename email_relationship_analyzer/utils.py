import re
import os
import logging
import pandas as pd
from config import REPLY_PREFIXES

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 编译正则表达式以提高性能
PREFIX_PATTERNS = [re.compile(prefix, re.IGNORECASE) for prefix in REPLY_PREFIXES]

def normalize_subject(subject):
    """标准化邮件主题，移除所有回复前缀"""
    if not isinstance(subject, str):
        return ""
        
    original_subject = subject.strip()
    
    # 使用通用复合前缀模式一次性处理所有前缀
    compound_pattern = PREFIX_PATTERNS[-1]
    cleaned_subject = compound_pattern.sub("", original_subject).strip()
    
    # 如果标准化后的主题为空，使用原始主题
    if not cleaned_subject:
        logger.warning(f"标准化后主题为空，使用原始主题: {subject}")
        return subject.strip()
        
    return cleaned_subject

def extract_domain(email):
    """提取邮箱域名"""
    try:
        # 断言：邮箱地址应该包含@符号
        assert isinstance(email, str) and '@' in email, f"无效的邮箱地址: {email}"
        return email.split('@')[1]
    except (IndexError, AttributeError, AssertionError) as e:
        return None

def extract_username(email):
    """提取邮箱用户名"""
    try:
        # 断言：邮箱地址应该包含@符号
        assert isinstance(email, str) and '@' in email, f"无效的邮箱地址: {email}"
        return email.split('@')[0]
    except (IndexError, AttributeError, AssertionError) as e:
        return None

def is_reply(subject):
    """判断是否为回复邮件"""
    if not isinstance(subject, str):
        return False
        
    for pattern in PREFIX_PATTERNS:
        if pattern.match(subject.strip()):
            return True
    return False

def validate_file(file_path):
    """验证文件是否存在并获取文件信息"""
    assert os.path.exists(file_path), f"文件不存在: {file_path}"
    
    file_size = os.path.getsize(file_path) / (1024 * 1024)  # 转换为MB
    logger.info(f"文件大小: {file_size:.2f} MB")
    
    # 根据文件大小决定分块大小
    chunksize = 10000 if file_size > 100 else 5000
    
    return file_size, chunksize 