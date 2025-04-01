# 配置信息和常量

# 定义必需的列
REQUIRED_COLUMNS = ['邮件名称', '发件人', '收件人', '邮件消息标识']

# 多语言回复前缀配置
REPLY_PATTERNS = {
    'zh': {
        'reply': ['回复', '答复', '回覆', 'Re'],
        'forward': ['转发', '轉發', 'Fw', 'Fwd']
    },
    'en': {
        'reply': ['Re', 'Reply', 'RE'],
        'forward': ['Fw', 'Fwd', 'Forward']
    },
    'chinese': {
        'reply': [r"回复", r"答复", r"回覆", r"返"],
        'forward': [r"转发", r"轉發"]
    },
    'korean': {
        'reply': [r"답장", r"답신", r"회신", r"답변"],
        'forward': [r"전달", r"전송", r"전달함"]
    },
    'japanese': {
        'reply': [r"返信", r"応答"],
        'forward': [r"転送"]
    },
    'german': {
        'reply': [r"AW", r"Antw"],
        'forward': [r"WG", r"Wg"]
    },
    'french': {
        'reply': [r"Rép", r"Rep"],
        'forward': [r"TR", r"Tr"]
    },
    'spanish': {
        'reply': [r"RV", r"Res"],
        'forward': [r"RV", r"Reenviar"]
    },
    'italian': {
        'reply': [r"Risp", r"R"],
        'forward': [r"I", r"Inoltr"]
    },
    'russian': {
        'reply': [r"Отв", r"Ответ", r"От"],
        'forward': [r"Пер", r"Переслать", r"Fwd"]
    }
}

# 语言字符集范围
LANGUAGE_RANGES = {
    'chinese': (0x4E00, 0x9FFF),     # 中文
    'korean': (0xAC00, 0xD7AF),      # 韩文
    'japanese': (0x3040, 0x30FF),    # 日文
    'russian': (0x0400, 0x04FF),     # 俄文西里尔字母
    'thai': (0x0E00, 0x0E7F),        # 泰文
    'arabic': (0x0600, 0x06FF),      # 阿拉伯文
    'hebrew': (0x0590, 0x05FF),      # 希伯来文
    'devanagari': (0x0900, 0x097F),  # 梵文
}

def detect_language(text):
    """
    检测文本可能的语言
    返回可能的语言列表和置信度
    """
    if not text:
        return None, 0
    
    language_counts = {lang: 0 for lang in LANGUAGE_RANGES.keys()}
    total_chars = 0
    
    for char in text:
        code_point = ord(char)
        for lang, (start, end) in LANGUAGE_RANGES.items():
            if start <= code_point <= end:
                language_counts[lang] += 1
                total_chars += 1
                break
    
    if total_chars == 0:
        if all(ord(c) < 128 for c in text):  # ASCII范围内
            return 'english', 1.0
        return None, 0
    
    # 计算每种语言的比例
    language_ratios = {
        lang: count/total_chars 
        for lang, count in language_counts.items() 
        if count > 0
    }
    
    # 获取比例最高的语言
    if language_ratios:
        main_language = max(language_ratios.items(), key=lambda x: x[1])
        return main_language[0], main_language[1]
    
    return None, 0

# 生成完整的回复前缀列表
def generate_reply_prefixes():
    prefixes = []
    
    # 从配置生成基本前缀
    for language in REPLY_PATTERNS.values():
        for prefix_type in ['reply', 'forward']:
            for prefix in language[prefix_type]:
                prefixes.append(fr"^{prefix}:\s*")
                prefixes.append(fr"^{prefix}：\s*")  # 支持全角冒号
    
    # 生成复合前缀（支持多级转发/回复）
    base_prefixes = [p.replace(r"^", "").replace(r"\s*", "") for p in prefixes]
    compound_patterns = []
    for p1 in base_prefixes:
        for p2 in base_prefixes:
            if p1 != p2:
                compound_patterns.append(fr"^{p1}{p2}\s*")
    
    prefixes.extend(compound_patterns)
    
    # 添加通用复合前缀模式
    all_prefixes = '|'.join([p.replace(r"^", "").replace(r"\s*", "") for p in prefixes])
    prefixes.append(fr"^(?:{all_prefixes}|\s)+")
    
    return prefixes

# 生成最终的回复前缀列表
REPLY_PREFIXES = generate_reply_prefixes()

# 默认输出文件
DEFAULT_OUTPUT_FILE = "relationships.json"
DEFAULT_ERROR_FILE = "error.json"
DEFAULT_UNKNOWN_FILE = "unknown.json"

# 日志格式
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s' 