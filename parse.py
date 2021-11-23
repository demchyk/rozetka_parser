import requests
import pandas as pd
from multiprocessing import Process
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool


class Parser(Process):
	def __init__(self):
		Process.__init__(self)
		self.__result_link = 'https://search.rozetka.com.ua/search/api/v6/'
		self.__param_dict = {'front-type': 'xl', 'country': 'UA', 'lang': 'ru'}
		self.__sku_list = self.__get_sku_from_file('sku.xlsx')
		self.__thread_num = 8
		self.__class__.__parse_withot_multithreading(self.__result_link, self.__param_dict, self.__sku_list).to_csv('rez.csv')
		self.__class__.__parse_with_multithreading(self.__result_link, self.__param_dict, self.__sku_list).to_csv('rez2.csv')
		self.__class__.__parse_seller_name(self.__param_dict, '5')



	@staticmethod
	def __get_json_response_with_parametr(link, param_dict):
		return requests.get(link, params = param_dict).json()

	@classmethod
	def __process_goods_json_to_df(cls, response, param_dict, sku):
		if response['data']['goods']:
			df = pd.json_normalize(response['data']['goods'])
			df['seller_name'] = df['seller_id'].apply(lambda x: cls.__parse_seller_name(param_dict, x))
			df['sku'] = str(sku)
			return df[['sku', 'title', 'price', 'seller_name', 'href', 'sell_status']]

	@classmethod
	def __parse_withot_multithreading(cls, link, param_dict, sku_list):
		df_list = []
		for item in sku_list:
			upd_dict = param_dict.copy()
			upd_dict.update({'text':str(item)})
			df_list.append(cls.__process_goods_json_to_df(cls.__get_json_response_with_parametr(link, upd_dict),param_dict, item))
		return pd.concat(df_list)


	@classmethod
	def __parse_seller_name(cls, param_dict, seller_id):
		link = 'https://product-api.rozetka.com.ua/v4/sellers/get'		
		upd_dict = param_dict.copy()
		upd_dict.update({'id': str(seller_id)})
		r = cls.__get_json_response_with_parametr(link, upd_dict)
		return r['data']['title']



	@staticmethod
	def __get_sku_from_file(path):
		return pd.read_excel(path, header=None, index_col=None)[0].tolist()

	@classmethod
	def __thread_wrapper(cls, link, param_dict, sku):
		upd_dict = param_dict.copy()
		upd_dict.update({'text':str(sku)})
		return cls.__process_goods_json_to_df(cls.__get_json_response_with_parametr(link, upd_dict),param_dict, sku)

	@classmethod
	def __parse_with_multithreading(self, link, param_dict, sku_list):
		with ThreadPool(8) as pool:
			self.__thread_wrapper = partial(self.__thread_wrapper, link, param_dict)
			result = pool.map(self.__thread_wrapper, sku_list)
		return pd.concat(result)


a = Parser()