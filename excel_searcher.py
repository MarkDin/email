import pandas as pd
import json
import logging
import argparse
from typing import Dict, List, Union, Any
import multiprocessing as mp
from functools import partial
import numpy as np

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def _search_chunk(chunk_data: pd.DataFrame, search_terms: List[str], columns: List[str], start_idx: int) -> List[Dict[str, Any]]:
    """在数据块中搜索
    
    Args:
        chunk_data (pd.DataFrame): 数据块
        search_terms (List[str]): 搜索关键词列表
        columns (List[str]): 列名列表
        start_idx (int): 起始行号
        
    Returns:
        List[Dict]: 搜索结果列表
    """
    results = []
    for idx, row in chunk_data.iterrows():
        matched_terms = {}
        for term in search_terms:
            matched_cols = []
            for col in columns:
                if term in str(row[col]).lower():
                    matched_cols.append(col)
            if matched_cols:
                matched_terms[term] = matched_cols
        
        if matched_terms:
            row_dict = row.to_dict()
            results.append({
                "row_index": start_idx + idx + 2,  # Excel行号从1开始，标题占用第1行
                "matched_terms": matched_terms,
                "data": row_dict
            })
    return results

def _column_search_chunk(chunk_data: pd.DataFrame, column_name: str, search_terms: List[str], start_idx: int) -> List[Dict[str, Any]]:
    """在数据块的指定列中搜索
    
    Args:
        chunk_data (pd.DataFrame): 数据块
        column_name (str): 列名
        search_terms (List[str]): 搜索关键词列表
        start_idx (int): 起始行号
        
    Returns:
        List[Dict]: 搜索结果列表
    """
    results = []
    for term in search_terms:
        matches = chunk_data[chunk_data[column_name].str.contains(term, case=False, na=False)]
        for idx, row in matches.iterrows():
            results.append({
                "row_index": start_idx + idx + 2,
                "matched_term": term,
                "data": row.to_dict()
            })
    return results

class ExcelSearcher:
    def __init__(self, excel_file: str):
        """初始化Excel搜索器
        
        Args:
            excel_file (str): Excel文件路径
        """
        self.df = pd.read_excel(excel_file)
        # 将所有数据转换为字符串，便于搜索
        self.df = self.df.astype(str)
        # 获取CPU核心数，留一个核心给系统
        self.num_processes = max(1, mp.cpu_count() - 1)
        
    def _split_dataframe(self) -> List[pd.DataFrame]:
        """将DataFrame分割成多个块
        
        Returns:
            List[pd.DataFrame]: DataFrame块列表
        """
        chunk_size = len(self.df) // self.num_processes
        if chunk_size == 0:
            return [self.df]
        return np.array_split(self.df, self.num_processes)
        
    def global_search(self, search_terms: List[str]) -> List[Dict[str, Any]]:
        """全局搜索（并行处理）
        
        Args:
            search_terms (List[str]): 要搜索的文本列表
            
        Returns:
            List[Dict]: 搜索结果列表，每个结果包含行号和匹配数据
        """
        # 处理搜索关键词
        search_terms = [term.strip().lower() for term in search_terms if term.strip()]
        if not search_terms:
            return []
            
        # 分割数据
        chunks = self._split_dataframe()
        columns = list(self.df.columns)
        
        # 创建进程池
        with mp.Pool(processes=self.num_processes) as pool:
            # 为每个块创建搜索任务
            search_tasks = []
            start_idx = 0
            for chunk in chunks:
                search_func = partial(_search_chunk, 
                                   search_terms=search_terms,
                                   columns=columns,
                                   start_idx=start_idx)
                search_tasks.append(pool.apply_async(search_func, (chunk,)))
                start_idx += len(chunk)
            
            # 收集所有结果
            results = []
            for task in search_tasks:
                results.extend(task.get())
            
        return results

    def column_search(self, column_name: str, search_terms: List[str]) -> List[Dict[str, Any]]:
        """在指定列中搜索（并行处理）
        
        Args:
            column_name (str): 列名
            search_terms (List[str]): 要搜索的文本列表
            
        Returns:
            List[Dict]: 搜索结果列表，每个结果包含行号和匹配数据
        """
        if column_name not in self.df.columns:
            raise ValueError(f"列名 '{column_name}' 不存在")
            
        # 处理搜索关键词
        search_terms = [term.strip().lower() for term in search_terms if term.strip()]
        if not search_terms:
            return []
            
        # 分割数据
        chunks = self._split_dataframe()
        
        # 创建进程池
        with mp.Pool(processes=self.num_processes) as pool:
            # 为每个块创建搜索任务
            search_tasks = []
            start_idx = 0
            for chunk in chunks:
                search_func = partial(_column_search_chunk,
                                   column_name=column_name,
                                   search_terms=search_terms,
                                   start_idx=start_idx)
                search_tasks.append(pool.apply_async(search_func, (chunk,)))
                start_idx += len(chunk)
            
            # 收集所有结果
            all_results = []
            for task in search_tasks:
                all_results.extend(task.get())
        
        # 去重（同一行可能匹配多个关键词）
        unique_results = []
        seen_rows = set()
        for result in all_results:
            if result["row_index"] not in seen_rows:
                seen_rows.add(result["row_index"])
                unique_results.append(result)
                
        return unique_results

    def save_results(self, results: List[Dict[str, Any]], output_file: str):
        """保存搜索结果到JSON文件
        
        Args:
            results (List[Dict]): 搜索结果
            output_file (str): 输出文件路径
        """
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

    def get_columns(self) -> List[str]:
        """获取所有列名
        
        Returns:
            List[str]: 列名列表
        """
        return list(self.df.columns)

def main():
    parser = argparse.ArgumentParser(description='Excel文件搜索工具')
    parser.add_argument('excel_file', help='Excel文件路径')
    parser.add_argument('search_text', help='要搜索的文本')
    parser.add_argument('--column', help='要搜索的列名（可选）')
    parser.add_argument('--output', default='search_results.json', help='输出JSON文件路径（默认：search_results.json）')
    
    args = parser.parse_args()
    
    try:
        searcher = ExcelSearcher(args.excel_file)
        
        if args.column:
            print(f"在列 '{args.column}' 中搜索 '{args.search_text}'...")
            results = searcher.column_search(args.column, args.search_text)
        else:
            print(f"全局搜索 '{args.search_text}'...")
            results = searcher.global_search(args.search_text)
        
        searcher.save_results(results, args.output)
        print(f"找到 {len(results)} 条匹配结果")
        print(f"结果已保存到：{args.output}")
        
    except Exception as e:
        print(f"错误：{str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    main() 