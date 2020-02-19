from django.shortcuts import render
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
import pandas as pd
import os
import logging
import datetime
from django.core.mail import send_mail
# Create your views here.
now_time = datetime.datetime.now()
logger = logging.getLogger('Submit_audit')
def main(request):
    return render(request,"index.html")

@csrf_exempt
def Data_processing(request):
    print('ok')
    print(request.method)
    if request.method == 'POST':
        try:
            for i in request.FILES:
                files = request.FILES.get(i)
                file_ = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'uploads/'+files.name)
                f = open(file_, "wb")
                for item in files.chunks():
                    f.write(item)
                f.close()
        except Exception as e:
            logger.info('url: %s method: %s 文件上传失败 原因：%s' % (request.path, request.method,e))
        
        
        res = send_mail('请您审核数据',
        '{0},提交数据，请您审核,审核地址为：{1}'.format(now_time,'http' + '://' + request.META['HTTP_HOST'] + '/' + 'audit' + '/'),
        'wang.zhaoyu@insobo.com',['m15733310520@163.com'],fail_silently = False)
        # 值1：邮件标题   值2：邮件主人  值3：发件人  值4：收件人  值5：如果失败，是否抛出错误 
        if res == 1:
            logger.info('url: %s method: %s 文件提交审核成功' % (request.path, request.method))
            return render(request, "index.html", {"item": "提交成功"})
            
        else:
            logger.info('url: %s method: %s 文件提交审核失败' % (request.path, request.method))
            return render(request, "index.html", {"item": "提交失败"})
            






