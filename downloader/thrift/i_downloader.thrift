#!/usr/local/bin/thrift --gen py -r -o ../thrift
#thrift --gen py   -out ../../../crawler/ i_downloader.thrift
namespace py bdp.i_crawler.i_downloader
struct SessionCommit
{
    1:optional string       refer_url,                                 #token页面url
    2:optional string       identifying_code_url,                      #验证码页面
    3:optional string       identifying_code_check_url,                #验证码结果检验
    4:optional map<string, string>  session_msg,                       #页面token名
    5:optional string       check_body,                                #检查response中是否包含
    6:optional string       check_body_not,                            #检查response中是否不包含
    7:optional bool         need_identifying                    =false,#检查response中是否不包含
}
struct Page
{
    1:optional string   url,                # 每次跳转url
    2:optional i32      compress_type,      # 解压类型
    3:optional string   content,            # 实际的内容
    4:optional i32      http_code,          # 每次下载的http_code
}
enum CrawlStatus {
    CRAWL_SUCCESS               = 0, # 抓取成功
    CRAWL_FAILT                 = 1, # 抓取失败
    CRAWL_RobotsDisallow        = 2, # robots封禁
    CRAWL_RedirectExceedLimit   = 3, # 重定向次数限制
    CRAWL_ConnectionFail        = 4, # 连接服务器失败
    CRAWL_SizetooSmall          = 5, # 网页太小
    CRAWL_IdentifyFail          = 6, # 验证码验证失败
    CRAWL_ProxyFail             = 7, # 代理使用失败
    CRAWL_ProxyDead             = 8, # 代理ping不通
}
struct Proxy
{
    1:required string   host,                # 代理ip
    2:required i32      port,                # 端口号
    3:optional string   user,                # 用户名
    4:optional string   password,            # 密码
    5:optional string   type,                # 类型
}
struct DownLoadReq
{
    1:required string                   url,                        # 必须属性，抓取url
    2:required string                   method          = 'get',    # 必须属性，抓取方式包括get，post，默认为get
    3:required string                   download_type   = 'simple', # 必须属性，下载类型：simple，phantom
    4:optional list<string>             ip_list,                    # 可选属性，该url的ip列表，可通过指定ip来抓取
    5:optional string                   src_type,                   # 可选属性，url来源
    6:optional i32                      priority,                   # 可选属性，该url的优先级
    7:optional i32                      retry_times                 # 可选属性，默认值为1
    8:optional i32                      time_out                    # 可选属性, 超时时间
    9:optional map<string, string>      post_data,                  # 可选属性，如果是post请求，则需要这个属性
    10:optional map<string, string>     http_header,                # 可选属性，http头的信息

    11:optional SessionCommit           session_commit,             # 可选属性，交互方式包括验证码
    12:optional bool                    verify          =  false,   # 可选属性，是否需要证书
    13:optional i32                     max_content_length,         # 可选属性，如果没有设置则采用系统参数值1M
    14:optional string                  user,                       # 可选属性，ftp下载用户名
    15:optional string                  password,                   # 可选属性，ftp下载用密码
    16:optional string                  scheduler,                  # 可选属性，调度相关信息
    17:optional string                  parse_extends,              # 可选属性，解析相关信息
    18:optional string                  data_extends,               # 可选属性，扩展属性
    19:optional Proxy                   proxy,                      # 可选属性，是否指定代理
    20:optional map<string, string>     info        = {},           # 可选属性，额外的信息
    21:optional bool                    use_proxy       = true,   # 是否使用代理
}

struct DownLoadRsp
{
    1:required string                   url,                   #必选字段，下载的url
    2:optional string                   redirect_url,          #必选字段，跳转url，无跳转则为跳转前url
    3:optional string                   src_type,              #可选字段，url来源             
    4:required CrawlStatus              status,                #必须字段，下载状态 
    5:required i32                      http_code,             #必须字段，http返回码
    6:optional i32                      download_time,         #可选字段，下载时间
    7:optional i32                      elapsed,               #可选字段，下载耗时
    8:list<Page>                        pages,                 #可选字段，下载的页面历史
    9:optional string                   content_type,          #可选字段，下载的类型
    10:optional string                  content,               #可选字段，网页的内容
    11:optional i32                     page_size,             #可选字段，网页的大小
    12:optional string                  scheduler,             #可选属性，调度相关信息
    13:optional string                  parse_extends,         #可选属性, 解析相关信息
    14:optional string                  data_extends,          #可选属性，扩展属性
    15:optional map<string, string>     info            = {},  #可选属性，额外的信息
}
struct RetStatus
{
    1:required i32                      status,                #0代表失败,1daibiaochenggou
    2:optional string                   errormessage,          #shibaixinxi
}

service DownloadService
{
    DownLoadRsp download(1:DownLoadReq req),
    RetStatus commit_download_task(1:DownLoadReq req),
    RetStatus download_task(1:DownLoadReq req),
}
