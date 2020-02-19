from django.shortcuts import render
from django.http import HttpResponse,FileResponse,JsonResponse
from django.views.decorators.csrf import csrf_exempt
import os
import zipfile
import io
import tempfile
from wsgiref.util import FileWrapper
import datetime
import logging
import threading
from audit import tasks
from task import data_stored



# import thread
logger = logging.getLogger('Submit_audit')
# Create your views here.
def Adiut_Data(request):
    file_name = os.listdir(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),'uploads'))
    
    return render(request,'audit.html',{'file_name':file_name})

@csrf_exempt
def Data_Download(request):
    if request.method == 'POST':
        try:
            # 创建BytesIO
            s = io.BytesIO()
            # 创建一个临时文件夹用来保存下载的文件
            temp = tempfile.TemporaryDirectory()
            # 使用BytesIO生成压缩文件
            zip = zipfile.ZipFile(s, 'w')
            for i in request.POST.getlist('boxes'):
                print(type(i))

                f_name = i[:-1]
                print(f_name)

                local_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),'uploads\\'+f_name)
                # # 下载文件
                # ur.urlretrieve(i['download_url'], local_path)
                # 把下载文件的写入压缩文件
                zip.write(local_path, f_name)
            # 关闭文件
            zip.close()
            # 指针回到初始位置，没有这一句前端得到的zip文件会损坏
            s.seek(0)
            # 用FileWrapper类来迭代器化一下文件对象.
            wrapper = FileWrapper(s)
            response = HttpResponse(wrapper, content_type='application/zip')
            response['Content-Disposition'] = 'attachment; fi   lename={}.zip'.format(datetime.datetime.now().strftime("%Y-%m-%d"))
            logger.info('url: %s method: %s 文件下载成功' % (request.path, request.method))
        except Exception as e:
            logger.info('url: %s method: %s 文件下载失败 原因：%s' % (request.path, request.method,e))
        
    return response


def Import_Data(request):

    if request.method == 'GET':
        file_name_1 = request.GET['file_name_1']
        file_name_2 = request.GET['file_name_2']
        file_name_3 = request.GET['file_name_3']
        print(file_name_1,file_name_2,file_name_3)
        for i in [file_name_1,file_name_2,file_name_3]:
            i = i[:-1]
            if '资产负债表' in i:
                file_1 = i
            if '现金流量表' in i:
                file_2 = i
            if '利润表' in i:
                file_3 = i
        file_1 = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),'uploads\\' + file_1)
        file_2 = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),'uploads\\' + file_2)
        file_3 = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),'uploads\\' + file_3)
        logger.info('url: %s method: %s 后台开始处理数据' % (request.path, request.method))
        t = threading.Thread(target=data_stored.go,args=(file_1,file_2,file_3))
        t.start()
        
        return JsonResponse({'status':'ok'})







