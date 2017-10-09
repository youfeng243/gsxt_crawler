**工商信息离线抓取系统**

总入口文件: start_task_crawler.py

同步种子脚本: start_update_process.py


###### 删除部分属性mongodb操作
* db.getCollection('online_all_search').update({province:'gansu'},{$unset:{'crawl_online':''}},false, true)