import requests
from lxml import etree
import pymongo, time
import threading
from queue import Queue


class Adbug():
    def __init__(self):
        self.other_url = 'https://testapi.adbug.cn/api/v9/get/aggs/data/new?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwOi8vdGVzdGFwaS5hZGJ1Zy5jbi91c2VyL2xvZ2luIiwiaWF0IjoxNTc4MzY1Mzk1LCJleHAiOjE1Nzg5NzAxOTUsIm5iZiI6MTU3ODM2NTM5NSwianRpIjoiZDhmOGI1N2U2OGZjZDBiMDNmMTA0YzI4ZjBiMDBmYzAiLCJzdWIiOjE3NDcwfQ.ChrwJ3jhu1BgmIFUeFLFPgha8lVjvRi0taniTBdn72w'
        self.home_url = 'https://testapi.adbug.cn/api/v9/get/ads/search?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwOi8vdGVzdGFwaS5hZGJ1Zy5jbi91c2VyL2xvZ2luIiwiaWF0IjoxNTc4MzY1Mzk1LCJleHAiOjE1Nzg5NzAxOTUsIm5iZiI6MTU3ODM2NTM5NSwianRpIjoiZDhmOGI1N2U2OGZjZDBiMDNmMTA0YzI4ZjBiMDBmYzAiLCJzdWIiOjE3NDcwfQ.ChrwJ3jhu1BgmIFUeFLFPgha8lVjvRi0taniTBdn72w'
        self.param_queue = Queue()
        self.res_queue = Queue()
        self.material_queue = Queue()  # 素材队列
        self.file_queue = Queue()  # 素材存储路径队列
        self.save_queue = Queue()

    # 热门媒体分类
    def media_function(self):
        print('正在生成品牌分类，稍等\n...')
        media_params = {
            'field': 'mainpublisher',
            'accurate': 2
        }
        social_res = requests.post(self.other_url, media_params).json()
        mainpublisher = social_res['data']['mainpublisher']
        nums = len(mainpublisher)
        # 热门媒体分类
        media_top_dict = {}
        for item in mainpublisher:
            media_top_dict[item['cname']] = item['category']

        media_dict = {}
        tag_list = set([v for k, v in media_top_dict.items()])
        for tag in tag_list:
            update = []
            for k, v in media_top_dict.items():
                if v == tag:
                    update.append(k)
                    media_dict['热门媒体--' + tag] = update
        return media_dict

    # 行业筛选
    def industry_function(self):
        industry_params = {
            'field': 'industry',
            'accurate': 2,
        }
        industry_dict = {}
        industry_list = []
        industry_res = requests.post(self.other_url, industry_params).json()
        industry = industry_res['data']['industry']
        for item in industry:
            industry_dict[item['cname']] = item['id']
        for item in industry:
            industry_list.append(item['cname'])
        return industry_list

    # 所有品牌
    def all_brands_function(self, industry_list, media_dict):

        all_brands_dict = {}
        for industry_name in industry_list:
            params = {
                'field': 'advertiser',
                'industry': industry_name,
                'accurate': 2,
            }
            all_brands_res = requests.post(self.other_url, params).json()
            advertiser = all_brands_res['data']['advertiser']
            all_brands_dict[industry_name] = set([item['brand'] for item in advertiser if item['brand']])
        for k, v in media_dict.items():
            all_brands_dict[k] = set(v)

        # all_brands_list = []
        # for k, v in all_brands_dict.items():
        #     for brand in v:
        #         all_brands_list.append(brand)
        return all_brands_dict

    # 获取请求url参数
    def get_param(self):
        print('正在生成请求参数')
        for page in range(61):
            home_params = {
                "page": page,
            }
            self.param_queue.put(home_params)

    # 获取单页请求数据
    def get_info(self):

        while True:
            home_params = self.param_queue.get()
            # print(home_params)
            respons = requests.post(self.home_url, home_params).json()
            # print(respons)
            self.res_queue.put(respons)
            self.param_queue.task_done()

    def get_home_data(self, all_brands_dict):
        print('数据细分')
        index = 0
        while True:
            home_res = self.res_queue.get()
            home_data = home_res['data']
            # 遍历当前页面数据，获取每条广告的具体数据
            for item in home_data:
                # print('item:',item)
                # id/创意详情url
                id = item['id']
                detaile_url = "https://www.adbug.cn/ad/" + id + ".html"
                # 素材类型
                material_type = item['type']
                # 投放平台
                attribute = item['attribute04']
                # 落地页名称/广告商
                advertiser_name = item['advertiser_name_title']
                # 1.set广告商，set品牌[0] 求交集  得分=len交集/len品牌
                # 生成所有品牌的list 分数为1的提取出来 放到all_brands_dict里面获取行业
                # 品牌归属
                brand_blongs = advertiser_name
                # 行业归属
                industry_blongs = 'NULL'
                if advertiser_name:  # 如果有广告商
                    set_adv = set(advertiser_name)
                    for indus, brandes in all_brands_dict.items():
                        for bran in brandes:
                            bra = bran.split(' ')[0]
                            if bra:
                                set_bra = set(bra)
                                intersection = set_adv & set_bra
                                if len(set_bra) == len(intersection):
                                    brand_blongs = bran
                                    industry_blongs = indus
                else:
                    brand_blongs = '其他'
                    industry_blongs = '其他'

                # 落地页域名
                advertiser_domain = item['advertiser_name']
                # 投放媒体
                publisher_full = item['publisher_full']
                # 投放媒体归属
                publisher_blongs = 'NULL'
                for k, v in all_brands_dict.items():
                    for bran in v:
                        if bran in publisher_full or publisher_full.split(' ')[1] in bran:
                            publisher_blongs = k
                # 创意名称
                title = item['title']
                # 广告科技
                advertising_technology = ','.join(item['tags_list'])
                if advertising_technology is not True:
                    advertising_technology = 'NULL'
                # 图片或视频地址
                img_or_video_address = item['am_source_url']  # 图片或者视频的拼接字段
                image_video_url = ''
                # 如果是图片，将图片存储到本地，并以标题命名，以.jpeg格式存储
                if material_type == 'image':
                    # image_path = '/home/adbug/images/' + id + '.jpeg'
                    image_path = './images/' + id + '.jpeg'

                    img_url = "https://file.adbug.cn/m/image/" + img_or_video_address + "?x-oss-process=image/resize,w_650/watermark,type_d3F5LXplbmhlaQ,size_22,text_QURCVUc=,color_FFFFFF,t_5,g_sw,x_10,y_10,p_10,fill_1,interval_80,rotate_45"
                    self.material_queue.put(img_url)
                    self.file_queue.put(image_path)

                    # 存储到数据库的图片数据
                    image_video_url = 'http://meta.houselai.com:88/images/' + id + '.jpeg'
                elif material_type == 'flv':
                    video_path = './video/' + id + '.mp4'
                    # video_path = '/home/adbug/video/' + id + '.mp4'
                    video_url = "https://file.adbug.cn/m/flv/" + img_or_video_address
                    self.material_queue.put(video_url)
                    self.file_queue.put(video_path)

                    # 存储到数据库的视频数据
                    image_video_url = 'http://meta.houselai.com:88/video/' + id + '.mp4'
                else:
                    image_video_url = '无数据 等待下次更新...'
                    print('material_type:', material_type)

                # 请求创意详情url -------------------------------------------请求创意详情url-------------------------------------
                html = requests.get(detaile_url).text
                xml = etree.HTML(html)
                # 首次发现时间
                fist_date = xml.xpath('//div[@class="ad-d-list"]//span[@class="cnt"]')[0].text
                # 最近发现时间
                last_date = xml.xpath('//div[@class="ad-d-list"]//span[@class="cnt"]')[1].text
                # 广告尺寸
                advertis_size = xml.xpath('//div[@class="ad-d-list"]//span[@class="cnt"]')[-1].text

                data_info = {
                    'id': id,
                    'material_type': material_type,  # img/video
                    'attribute': attribute,  # 投放平台
                    'advertiser_name': advertiser_name,  # 落地页名称/广告商
                    'brand_blongs': brand_blongs,  # 品牌归属
                    'industry_blongs': industry_blongs,  # 行业归属
                    'advertiser_domain': advertiser_domain,  # 落地页域名
                    'publisher_full': publisher_full,  # 投放媒体
                    'publisher_blongs': publisher_blongs,  # 投放媒体归属
                    'title': title,  # 创意名称
                    'advertising_technology': advertising_technology,  # 广告科技
                    'image_video_url': image_video_url,  # 图片或视频链接
                    'fist_date': fist_date,  # 首次发现时间
                    'last_date': last_date,  # 最近发现时间
                    'advertis_size': advertis_size,  # 广告尺寸
                }
                self.save_queue.put(data_info)
            self.res_queue.task_done()
            index += 1
            print(index)

    # 存储数据
    def sava_info(self, p):
        print('存储数据中\n...')
        nums = 1
        update_num = 0
        insert_num = 0
        while True:
            material_url = self.material_queue.get()
            material_file = self.file_queue.get()
            data_info = self.save_queue.get()

            id = data_info['id']
            last_date = data_info['last_date']
            image_video_url = data_info['image_video_url']
            is_exist = p.count_documents({'id': id})
            # print('数据库是否有数据', is_exist)
            if is_exist:
                p.update_one({"id": id},
                             {"$set": {"last_date": last_date}})

                p.update_one({"id": id}, {"$set": {"image_video_url": image_video_url}})
                update_num += 1
                print('修改成功' + str(update_num))
            else:
                result = requests.get(material_url).content
                with open(material_file, 'ab') as w:
                    w.write(result)
                    w.close()
                p.insert_many([data_info])
                insert_num += 1
                print('添加成功' + str(insert_num))

            self.material_queue.task_done()
            self.file_queue.task_done()
            self.save_queue.task_done()

            nums += 1
        print('共添加' + str(insert_num) + '条数据')
        print('共更新' + str(update_num) + '条数据')

    # mongo工具
    def mongo_client(self):
        client = pymongo.MongoClient(host='meta.houselai.com', port=27018)
        db = client.advertising_db
        p = db.adbug

        return p

    def main(self):
        media_dict = self.media_function()
        industry_list = self.industry_function()
        all_brands_dict = self.all_brands_function(industry_list, media_dict)
        p = self.mongo_client()

        threads = []
        # 获取请求参数线程
        t_param = threading.Thread(target=self.get_param)
        threads.append(t_param)
        # 获取请求数据线程
        for i in range(6):
            t_info = threading.Thread(target=self.get_info)
            threads.append(t_info)
        # 数据细分线程
        t_data = threading.Thread(target=self.get_home_data, args=(all_brands_dict,))
        threads.append(t_data)
        # 存储数据
        t_save = threading.Thread(target=self.sava_info, args=(p,))
        threads.append(t_save)

        for t in threads:
            t.setDaemon(True)
            t.start()

        for q in [self.param_queue, self.res_queue, self.material_queue, self.file_queue, self.save_queue]:
            q.join()


if __name__ == '__main__':
    start_time = time.time()
    adbug = Adbug()
    adbug.main()
    over_time = time.time()
    print('共计用时：', over_time - start_time)
