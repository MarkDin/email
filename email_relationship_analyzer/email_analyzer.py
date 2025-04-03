import json
import logging
import pandas as pd
import os
from tqdm import tqdm
from config import REQUIRED_COLUMNS
from utils import normalize_subject, extract_domain, extract_username, is_reply, validate_file
from langdetect import detect

# 设置日志
logger = logging.getLogger(__name__)

class EmailRelationshipAnalyzer:
    def __init__(self, excel_file_path):
        """初始化分析器"""
        self.excel_file_path = excel_file_path
        self.relationships = {}  # 存储关系集合
        self.unknown_data = {
            "invalid_emails": [],      # 无效的邮箱格式
            "empty_data": [],          # 空数据
            "multiple_senders": {},    # 相同主题不同发件人
            "invalid_recipients": [],  # 无效的收件人格式
            "processing_errors": [],   # 处理过程中的错误
            "unknown_languages": [],    # 未知语言的邮件
            "processing_notes": []     # 处理过程中的备注
        }
        self.required_columns = REQUIRED_COLUMNS
        self.subject_sender_map = {}  # 用于跟踪相同主题的不同发件人
        self.pending_replies = {}  # 用于暂存找不到原始邮件的回复
        self.original_emails = {}  # 用于存储所有原始邮件 {subject: [original_email_data]}
    
    def validate_data(self, chunk):
        """验证数据块是否包含所有必需的列"""
        missing_columns = [col for col in self.required_columns if col not in chunk.columns]
        if missing_columns:
            raise ValueError(f"Excel文件缺少必需的列: {missing_columns}")
        
        # 只保留需要的列，忽略其他列
        return chunk[self.required_columns]
    
    def process_chunk(self, chunk):
        """处理一个数据块"""
        try:
            # 验证并过滤数据
            chunk = self.validate_data(chunk)
            
            # 断言：验证后的数据块不应该为空
            assert not chunk.empty, "处理后的数据块为空"
            
            # 标准化主题和识别回复
            logger.info(f"开始标准化主题和识别回复")
            chunk['normalized_subject'] = chunk['邮件名称'].apply(lambda x: normalize_subject(x) if pd.notna(x) else "")
            chunk['is_reply'] = chunk['邮件名称'].apply(lambda x: is_reply(x) if pd.notna(x) else False)
            
            # 过滤掉没有邮件消息标识的行
            missing_ids = chunk[chunk['邮件消息标识'].isna()]
            if not missing_ids.empty:
                logger.warning(f"发现 {len(missing_ids)} 行缺少邮件消息标识，使用行号作为替代")
                # 为缺少邮件消息标识的行添加行号作为标识
                for idx, row in missing_ids.iterrows():
                    subject_info = row['邮件名称'] if pd.notna(row['邮件名称']) else "未知主题"
                    # 记录到unknown_data中
                    self.unknown_data["empty_data"].append({
                        "row_index": idx,
                        "subject": subject_info,
                        "error": "缺少邮件消息标识，使用行号替代"
                    })
                    # 使用行号作为邮件消息标识
                    chunk.loc[idx, '邮件消息标识'] = f"ROW_{idx}"
                
                # 过滤保留有邮件消息标识的行
                chunk = chunk[chunk['邮件消息标识'].notna()]
                
                # 如果过滤后没有数据，则返回
                if chunk.empty:
                    logger.warning("过滤后没有有效数据")
                    return
            
            # 按标准化主题分组处理
            logger.info(f"开始按标准化主题分组处理")
            for subject, group in chunk.groupby('normalized_subject'):
                if not subject:  # 跳过空主题
                    logger.warning("标准化后主题为空，使用原始主题")
                    continue
                    
                # 找出原始邮件（非回复）和回复邮件，并转换为字典列表
                original_emails = group[~group['is_reply']].to_dict('records')
                reply_emails = group[group['is_reply']].to_dict('records')
                
                # 断言：分组后的数据不应该为空
                assert len(original_emails) > 0 or len(reply_emails) > 0, f"主题为'{subject}'的分组为空"
                
                # 处理原始邮件
                if original_emails:
                    # 将原始邮件添加到缓存中
                    if subject not in self.original_emails:
                        self.original_emails[subject] = []
                    self.original_emails[subject].extend(original_emails)
                    
                    # 处理原始邮件的收件人关系
                    for original_email in original_emails:
                        self._process_original_email(original_email, subject)
                
                # 处理回复邮件
                for reply in reply_emails:
                    # 检查是否有对应的原始邮件
                    if subject in self.original_emails:
                        # 有原始邮件，直接处理
                        self._process_reply_email(reply, subject)
                    else:
                        # 没有找到原始邮件，暂存到pending_replies
                        if subject not in self.pending_replies:
                            self.pending_replies[subject] = []
                        self.pending_replies[subject].append(reply)
        
        except AssertionError as e:
            # 记录断言错误
            logger.error(f"断言错误: {str(e)}")
            self.unknown_data["processing_errors"].append({
                "chunk_hash": hash(str(chunk.iloc[0].to_dict())),
                "error": f"断言错误: {str(e)}",
                "type": "assertion_error"
            })
        
        except Exception as e:
            # 记录其他错误
            logger.error(f"处理数据块时发生错误: {str(e)}")
            self.unknown_data["processing_errors"].append({
                "chunk_hash": hash(str(chunk.iloc[0].to_dict())),
                "error": str(e),
                "type": "chunk_processing_error"
            })
    
    def _get_safe_send_time(self, email_data):
        """安全获取发送时间
        
        Args:
            email_data: 邮件数据字典
            
        Returns:
            str: 发送时间，如果不存在则返回空字符串
        """
        try:
            send_time = email_data.get('发送时间', '')
            return str(send_time) if pd.notna(send_time) else ''
        except (KeyError, AttributeError):
            return ''

    def _process_original_email(self, original_email, subject):
        """处理原始邮件"""
        sender = original_email['发件人']
        if '收件人' in original_email and pd.notna(original_email['收件人']):
            recipients = str(original_email['收件人']).split(',')
            has_valid_recipient = False  # 标记是否有有效的收件人
            
            for recipient in recipients:
                recipient = recipient.strip()
                if not isinstance(recipient, str) or '@' not in recipient:
                    self.unknown_data["invalid_emails"].append({
                        "message_id": original_email['邮件消息标识'],
                        "email": recipient,
                        "error": "收件人格式不正确",
                        "subject": subject
                    })
                    continue
                
                # 提取收件人域名
                recipient_domain = extract_domain(recipient)
                if not recipient_domain:
                    self.unknown_data["invalid_emails"].append({
                        "message_id": original_email['邮件消息标识'],
                        "email": recipient,
                        "error": "无法提取收件人域名",
                        "subject": subject
                    })
                    continue
                
                # 获取收件人用户名
                username = extract_username(recipient)
                if not username:
                    self.unknown_data["invalid_emails"].append({
                        "message_id": original_email['邮件消息标识'],
                        "email": recipient,
                        "error": "无法提取用户名",
                        "subject": subject
                    })
                    continue
                
                # 构建关系键
                key = f"{sender}#{subject}#{recipient_domain}"
                
                # 获取发送时间
                send_time = self._get_safe_send_time(original_email)
                
                # 构建关系值
                value = (sender, subject, username, original_email['邮件消息标识'], send_time)
                
                # 添加到关系集合
                if key not in self.relationships:
                    self.relationships[key] = []
                
                if value not in self.relationships[key]:
                    self.relationships[key].append(value)
                    has_valid_recipient = True
                    logger.info(f"成功添加关系: key='{key}'")
            
            if not has_valid_recipient:
                logger.warning(f"邮件 {original_email['邮件消息标识']} 的所有收件人都无效")

    def _analyze_missing_original_email(self, reply_subject, reply_title):
        """分析找不到原始邮件的原因
        
        Args:
            reply_subject: 标准化后的主题
            reply_title: 原始邮件标题
        
        Returns:
            str: 分析结果
        """
        # 检查是否存在相似主题
        similar_subjects = []
        for subject in self.original_emails.keys():
            # 计算相似度（这里用简单的包含关系作为示例）
            if reply_subject in subject or subject in reply_subject:
                similar_subjects.append(subject)
        
        if similar_subjects:
            return f"找到可能相关的主题: {', '.join(similar_subjects)}"
        
        # 检查标准化是否正确
        normalized = normalize_subject(reply_title)
        if normalized != reply_subject:
            return f"主题标准化可能有问题: 原标题='{reply_title}', 标准化后='{normalized}', 当前使用的标准化主题='{reply_subject}'"
        
        return "完全找不到相关的原始邮件主题"

    def _process_reply_email(self, reply, subject):
        """处理回复邮件"""
        replier = reply['发件人']
        
        # 验证回复者格式
        if not isinstance(replier, str) or '@' not in replier:
            self.unknown_data["invalid_emails"].append({
                "message_id": reply['邮件消息标识'],
                "email": replier,
                "error": "回复者格式不正确",
                "subject": subject
            })
            return
        
        # 提取回复者域名
        reply_domain = extract_domain(replier)
        if not reply_domain:
            self.unknown_data["invalid_emails"].append({
                "message_id": reply['邮件消息标识'],
                "email": replier,
                "error": "无法提取回复者域名",
                "subject": subject
            })
            return
        
        # 断言：域名不应该为空
        assert reply_domain, f"提取的域名为空: {replier}"
        
        # 获取回复者用户名
        username = extract_username(replier)
        
        # 断言：用户名不应该为空
        assert username, f"提取的用户名为空: {replier}"
        
        # 记录当前处理的回复邮件信息
        logger.info(f"正在处理回复邮件: subject='{subject}', replier='{replier}', message_id='{reply['邮件消息标识']}'")
        
        # 检查原始邮件缓存
        if subject not in self.original_emails:
            analysis_result = self._analyze_missing_original_email(subject, reply['邮件名称'])
            logger.warning(f"主题 '{subject}' 在原始邮件缓存中不存在，分析结果: {analysis_result}")
            self.unknown_data["processing_errors"].append({
                "message_id": reply['邮件消息标识'],
                "error": "找不到对应主题的原始邮件",
                "subject": subject,
                "replier": replier,
                "send_time": self._get_safe_send_time(reply),
                "original_title": reply['邮件名称'],
                "analysis": analysis_result
            })
            return
        
        # 找到对应的原始邮件
        original_email = None
        for email in self.original_emails[subject]:
            # 检查收件人是否包含回复者
            if '收件人' in email and pd.notna(email['收件人']):
                recipients = str(email['收件人']).split(',')
                if any(replier in recipient.strip() for recipient in recipients):
                    original_email = email
                    logger.info(f"找到匹配的原始邮件: sender='{email['发件人']}', message_id='{email['邮件消息标识']}'")
                    break
        
        # 如果找到对应的原始邮件，创建关系
        if original_email:
            original_sender = original_email['发件人']
            
            # 构建关系键
            key = f"{original_sender}#{subject}#{reply_domain}"
            
            # 获取发送时间
            send_time = self._get_safe_send_time(reply)
            
            # 构建关系值
            value = (original_sender, subject, username, reply['邮件消息标识'], send_time)
            
            # 添加到关系集合
            if key not in self.relationships:
                self.relationships[key] = []
            
            if value not in self.relationships[key]:
                self.relationships[key].append(value)
                logger.info(f"成功添加关系: key='{key}'")
        else:
            # 如果找不到对应的原始邮件，记录错误
            logger.warning(f"在主题 '{subject}' 的 {len(self.original_emails[subject])} 个原始邮件中没有找到收件人包含 '{replier}' 的邮件")
            self.unknown_data["processing_errors"].append({
                "message_id": reply['邮件消息标识'],
                "error": "找不到对应的原始邮件",
                "subject": subject,
                "replier": replier,
                "send_time": self._get_safe_send_time(reply),
                "original_emails_count": len(self.original_emails[subject])
            })

    def process_pending_replies(self):
        """处理所有暂存的回复邮件"""
        for subject, replies in self.pending_replies.items():
            if subject in self.original_emails:
                # 找到了原始邮件，处理所有暂存的回复
                for reply in replies:
                    self._process_reply_email(reply, subject)
            else:
                # 没有找到原始邮件，将所有回复邮件作为独立的关系处理
                logger.info(f"主题 '{subject}' 没有原始邮件，有 {len(replies)} 个回复，尝试建立独立关系")
                
                # 按发送时间排序（如果有）
                sorted_replies = sorted(
                    replies,
                    key=lambda x: pd.to_datetime(x.get('发送时间', '9999-12-31'), errors='coerce')
                )
                
                # 处理每个回复邮件
                for reply in sorted_replies:
                    # 验证发件人
                    sender = reply.get('发件人', '')
                    if not isinstance(sender, str) or '@' not in sender:
                        self.unknown_data["invalid_emails"].append({
                            "message_id": reply['邮件消息标识'],
                            "email": sender,
                            "error": "发件人格式不正确",
                            "subject": subject
                        })
                        continue
                    
                    # 获取收件人列表
                    recipients = str(reply.get('收件人', '')) if pd.notna(reply.get('收件人')) else ''
                    if not recipients:
                        self.unknown_data["invalid_emails"].append({
                            "message_id": reply['邮件消息标识'],
                            "error": "收件人为空",
                            "subject": subject
                        })
                        continue
                    
                    has_valid_recipient = False  # 标记是否有有效的收件人
                    # 处理收件人列表
                    for recipient in recipients.split(','):
                        recipient = recipient.strip()
                        if not isinstance(recipient, str) or '@' not in recipient:
                            self.unknown_data["invalid_emails"].append({
                                "message_id": reply['邮件消息标识'],
                                "email": recipient,
                                "error": "收件人格式不正确",
                                "subject": subject
                            })
                            continue
                        
                        # 提取收件人域名
                        recipient_domain = extract_domain(recipient)
                        if not recipient_domain:
                            self.unknown_data["invalid_emails"].append({
                                "message_id": reply['邮件消息标识'],
                                "email": recipient,
                                "error": "无法提取收件人域名",
                                "subject": subject
                            })
                            continue
                        
                        # 获取收件人用户名
                        username = extract_username(recipient)
                        if not username:
                            self.unknown_data["invalid_emails"].append({
                                "message_id": reply['邮件消息标识'],
                                "email": recipient,
                                "error": "无法提取用户名",
                                "subject": subject
                            })
                            continue
                        
                        # 构建关系键
                        key = f"{sender}#{subject}#{recipient_domain}"
                        
                        # 构建关系值
                        value = (sender, subject, username, reply['邮件消息标识'], self._get_safe_send_time(reply))
                        
                        # 添加到关系集合
                        if key not in self.relationships:
                            self.relationships[key] = []
                        
                        if value not in self.relationships[key]:
                            self.relationships[key].append(value)
                            has_valid_recipient = True
                            logger.info(f"为独立回复邮件创建关系: key='{key}'")
                    
                    if not has_valid_recipient:
                        logger.warning(f"邮件 {reply['邮件消息标识']} 的所有收件人都无效")
                
                # 记录这种特殊处理
                self.unknown_data["processing_notes"].append({
                    "subject": subject,
                    "note": "没有找到原始邮件，已将回复邮件作为独立关系处理",
                    "reply_count": len(replies),
                    "message_ids": [reply['邮件消息标识'] for reply in sorted_replies]
                })

    def analyze(self):
        """分析Excel数据"""
        logger.info(f"开始分析文件: {self.excel_file_path}")
        
        try:
            # 验证文件
            file_size_mb, _ = validate_file(self.excel_file_path)
            
            # 检查文件表头以验证必需的列
            df_header = pd.read_excel(self.excel_file_path, nrows=0)
            missing_columns = [col for col in self.required_columns if col not in df_header.columns]
            
            if missing_columns:
                raise ValueError(f"Excel文件缺少必需的列: {missing_columns}")
            
            # 根据文件大小决定处理方式
            if file_size_mb > 500:  # 大文件（>500MB）
                logger.info(f"文件较大 ({file_size_mb:.2f}MB)，使用分块处理方式")
                self._analyze_large_file()
            else:
                logger.info(f"文件大小适中 ({file_size_mb:.2f}MB)，一次性读取处理")
                self._analyze_regular_file()
            
            # 处理暂存的回复邮件
            self.process_pending_replies()
            
            # 检查是否找到了关系
            if not self.relationships:
                logger.warning("未找到任何关系集合")
            else:
                logger.info(f"分析完成，找到 {len(self.relationships)} 个关系集合")
                logger.info(f"异常数据统计:")
                logger.info(f"- 无效邮箱: {len(self.unknown_data['invalid_emails'])}")
                logger.info(f"- 空数据: {len(self.unknown_data['empty_data'])}")
                logger.info(f"- 多发件人主题: {len(self.unknown_data['multiple_senders'])}")
                logger.info(f"- 处理警告: {len(self.unknown_data['processing_errors'])}")
                logger.info(f"- 未知语言: {len(self.unknown_data['unknown_languages'])}")
                
        except Exception as e:
            logger.error(f"分析过程中出错: {str(e)}")
            self.unknown_data["processing_errors"].append({
                "error": "分析过程错误",
                "details": str(e)
            })
            raise
    
    def _analyze_regular_file(self):
        """分析常规大小的文件（一次性读取）"""
        # 读取整个Excel文件
        df = pd.read_excel(
            self.excel_file_path,
            engine='openpyxl',
            usecols=lambda x: x in self.required_columns
        )
        
        logger.info(f"读取了 {len(df)} 条邮件记录")
        
        # 将数据分成多个小块以显示进度
        chunk_size = 1000  # 每个块的大小
        chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
        
        # 使用tqdm显示进度，处理每个数据块
        for i, chunk in enumerate(tqdm(chunks, desc="处理数据块")):
            try:
                self.process_chunk(chunk)
            except Exception as e:
                logger.error(f"处理数据块 {i} 时发生错误: {str(e)}")
                self.unknown_data["processing_errors"].append({
                    "error": "处理数据错误",
                    "details": str(e)
                })
    
    def _analyze_large_file(self):
        """分析大型文件（使用分块读取）"""
        # 分块读取文件
        chunksize = 10000
        reader = pd.read_excel(
            self.excel_file_path,
            chunksize=chunksize,
            engine='openpyxl',
            usecols=lambda x: x in self.required_columns
        )
        
        # 处理每个数据块
        for i, chunk in enumerate(tqdm(reader, desc="处理数据块")):
            try:
                self.process_chunk(chunk)
            except Exception as e:
                logger.error(f"处理数据块 {i} 时发生错误: {str(e)}")
                self.unknown_data["processing_errors"].append({
                    "chunk": i,
                    "error": str(e),
                    "type": "chunk_processing_error"
                })
    
    def save_relationships(self, output_file):
        """保存关系集合到JSON文件"""
        try:
            # 检查关系集合是否为空
            if not self.relationships:
                logger.warning("关系集合为空，没有内容可保存")
                return
            
            # 转换格式并按count排序
            formatted_relationships = {}
            
            # 1. 转换格式: 添加count字段和items字段
            for key, items in self.relationships.items():
                formatted_relationships[key] = {
                    "count": len(items),
                    "items": items
                }
            
            # 2. 按count从大到小排序
            sorted_relationships = dict(
                sorted(
                    formatted_relationships.items(),
                    key=lambda x: x[1]["count"],
                    reverse=True
                )
            )
            
            # 保存到JSON文件
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(sorted_relationships, f, ensure_ascii=False, indent=2)
            
            logger.info(f"关系集合已保存到: {output_file}")
        except Exception as e:
            logger.error(f"保存关系集合时出错: {str(e)}")
            self.unknown_data["processing_errors"].append({
                "error": "保存关系集合错误",
                "details": str(e)
            })
    
    def save_unknown_data(self, unknown_file='unknown.json'):
        """保存异常数据到JSON文件"""
        try:
            with open(unknown_file, 'w', encoding='utf-8') as f:
                json.dump(self.unknown_data, f, ensure_ascii=False, indent=2)
            logger.info(f"异常数据已保存到: {unknown_file}")
        except Exception as e:
            logger.error(f"保存异常数据时出错: {str(e)}") 