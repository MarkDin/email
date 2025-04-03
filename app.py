from flask import Flask, request, jsonify, send_file
from excel_searcher import ExcelSearcher
import os
import tempfile
import atexit
import shutil

app = Flask(__name__, 
           static_url_path='',  # 设置空的URL路径前缀
           static_folder='static')  # 指定静态文件目录

app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 400 * 1024 * 1024  # 400MB max-limit

# 确保上传目录存在
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# 存储当前的ExcelSearcher实例和临时目录
current_searcher = None
temp_dir = None

def cleanup():
    """清理临时文件和目录"""
    global temp_dir
    if temp_dir and os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

# 注册程序退出时的清理函数
atexit.register(cleanup)

@app.route('/')
def index():
    return app.send_static_file('index.html')

@app.route('/upload', methods=['POST'])
def upload():
    global current_searcher, temp_dir
    
    if 'file' not in request.files:
        return jsonify({'error': '没有上传文件'}), 400
        
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': '没有选择文件'}), 400
        
    if not file.filename.endswith(('.xlsx', '.xls')):
        return jsonify({'error': '只支持Excel文件'}), 400
    
    try:
        # 清理旧的临时目录
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        
        # 创建新的临时目录
        temp_dir = tempfile.mkdtemp()
        temp_file = os.path.join(temp_dir, file.filename)
        file.save(temp_file)
        
        # 创建新的搜索器实例
        current_searcher = ExcelSearcher(temp_file)
        
        # 返回列名列表
        return jsonify({
            'columns': current_searcher.get_columns()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/search', methods=['POST'])
def search():
    global current_searcher
    
    if current_searcher is None:
        return jsonify({'error': '请先上传文件'}), 400
    
    try:
        data = request.get_json()
        column_name = data.get('column_name', '')
        search_terms = data.get('search_terms', [])
        
        if not search_terms:
            return jsonify({'error': '请输入搜索内容'}), 400
        
        # 执行搜索
        if column_name:
            results = current_searcher.column_search(column_name, search_terms)
        else:
            results = current_searcher.global_search(search_terms)
            
        if len(results) > 100:
            msg = f"搜索结果过多，无法展示，数量：{len(results)}"
            return jsonify({'error': msg}), 400
            
        return jsonify(results)
        
    except Exception as e:
        # 如果出现异常，检查是否是因为文件不存在
        if isinstance(e, (FileNotFoundError, IOError)):
            current_searcher = None
            return jsonify({'error': '文件已失效，请重新上传'}), 400
        return jsonify({'error': str(e)}), 500

@app.route('/download/<filename>')
def download(filename):
    """下载搜索结果文件"""
    try:
        return send_file(filename, as_attachment=True, download_name='search_results.json')
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        # 删除临时文件
        if os.path.exists(filename):
            os.remove(filename)

if __name__ == '__main__':
    app.run(debug=True, port=5000) 