#!/usr/bin/env python3
#importing necessary libraries.
#test comment
from flask import Flask, jsonify, request
from flask_restful import Resource, Api 
import copy
import json
import os
import pandas as pd

product_json = []
with open('data.json') as f:
    for lines in f.readlines():
        product_json.append(json.loads(lines))

#reading json data as dataframe.
df = pd.read_json('data.json',lines=True,orient='columns')
#adding discount column to th dataframe.
df['discount'] = df['price'].apply(lambda x: (x['regular_price']['value']*100 - x['offer_price']['value']*100)/x['regular_price']['value'])
#fetching brand name and making it as a seperate column.
df['brand.name'] = df['brand'].apply(lambda x: x['name'])

website_id = ['5d0cc7b68a66a100014acdb0','5da94e940ffeca000172b12a','5da94ef80ffeca000172b12c','5da94f270ffeca000172b12e','5da94f4e6d97010001f81d72']

#adding competitors_prices column to dataframe which contain list of prices of all compitatior products for each NAP product.
competitors_prices = []
for i in range(len(df)):
    temp = []
    for k in website_id:
        if len(df['similar_products'][i]['website_results'][k]['knn_items']) != 0:
            price = df['similar_products'][i]['website_results'][k]['knn_items'][0]['_source']['price']['basket_price']['value']
            temp.append(price)
    competitors_prices.append(temp)
df['competitors_prices'] = competitors_prices

#Fetching basket price for each NAP product and storing it in a spereate column.        
df['basket_price'] = df['price'].apply(lambda x: x['basket_price']['value'])

#adding competition column which is list of website_id of all compitators for each NAP product
competition = []
for i in range(len(df)):
    temp = []
    for k in website_id:
        if len(df['similar_products'][i]['website_results'][k]['knn_items']) != 0:
            comp = df['similar_products'][i]['website_results'][k]['knn_items'][0]['_source']['website_id']
            temp.append(comp)
    competition.append(temp)
df['competition'] = competition

#making is_undercut column, it will tell if the NAP product is being sold at a price higher than any of the compitators
#is_undercut = True if NAP product is being sold at a price higher than any of the compitators
#is_undercut = False if price of NAP product is less than all compitator products.
is_undercut = [0]*len(df)
for i in range(len(df)):
    is_undercut[i] = not all(x > df['basket_price'][i] for x in df['competitors_prices'][i])
    
df['is_undercut'] = is_undercut
df['is_undercut'] = df['is_undercut'].apply(lambda x: str(x))

df['id'] = df['_id'].apply(lambda x: x['$oid'])

#function to parse the receiving json data. input is fetched json request.
#Each filter is stored in a list and all the list are stored in another list.
#Returns list of list containg all filters.
def parse_request(data):

    length = len(data)
    final = []
    if length == 1:
        return final
    else:
        for i in range(len(data['filters'])):
            ops = []
            ops.append(data['filters'][i]['operand1'])
            ops.append(data['filters'][i]['operator'])
            ops.append(data['filters'][i]['operand2'])
            final.append(ops)
        return final

#parse query_type        
def parse_query_type(data):

    return data['query_type']

# Function to check comparision operator( '==','<','>')
# takes input as output of parse_request and a dataframe to work upon.
def to_check_operator(l,_df):

	# If operand1 is 'compition', then a new column is created is_competition in dataframe 
	# which will be True if that the given website_id in operand2 is a compatitor for given NAP product.
    if l[0] == 'competition':

        is_competition = []

        for i in _df[l[0]]:
            if l[2] in i:
                is_competition.append(True)
            else:
                is_competition.append(False)

        _df['is_competition'] = is_competition
        _df = _df[_df['is_competition'] == True]
        return _df

     # If operand1 is 'discount_diff', then a new column is created discount_diff in dataframe
     # discount_diff will calculate percentage difference between basket_prices of the NAP product and the competing product mentioned in json request.
    elif l[0] == 'discount_diff':

    	_df.reset_index(drop = True, inplace = True)
    	single_compt_price = []

    	for i in range(len(_df)):
            if len(_df['similar_products'][i]['website_results'][_df['competition'][0][0]]['knn_items']) == 0:
                single_compt_price.append('NaN')
            else:
                price = _df['similar_products'][i]['website_results'][_df['competition'][0][0]]['knn_items'][0]['_source']['price']['basket_price']['value']
                single_compt_price.append(price)

    	_df['single_compt_price'] = single_compt_price
    	_df['basket_price'] = _df['price'].apply(lambda x: x['basket_price']['value'])
    	_df = _df[_df['single_compt_price'] != 'NaN']
    	_df['discount_diff'] = (_df['basket_price'] - _df['single_compt_price'])*100/_df['basket_price']

    	#handling comaprision operators for discount_diff
    	if l[1] == '==':
            _df = _df[_df[l[0]] == l[2]]
    	elif l[1] == '>':
            _df = _df[_df[l[0]] > l[2]]
    	elif l[1] == '<':
            _df = _df[_df[l[0]] < l[2]]
    	return _df
    
    #handling comaprision operators.
    else:
        if l[1] == '==':
            _df = _df[_df[l[0]] == l[2]]
        elif l[1] == '>':
            _df = _df[_df[l[0]] > l[2]]
        elif l[1] == '<':
            _df = _df[_df[l[0]] < [2]]
        return _df

#takes input as output of parse_request(data)(list of list)
#function to check length of filter and then calling to_check_operator on each filter one by one.
#it can handle filiter length of 0,1,2,3,4.
def check_length(l):

    df_ = copy.deepcopy(df)

    #if length of given query is 0
    if len(l) == 0:
        return df
    if len(l) == 1:
        _df = to_check_operator(l[0], df_)
        return _df

    elif len(l) == 2:
        df_1 = to_check_operator(l[0], df_)
        _df = to_check_operator(l[1], df_1)
        return _df

    elif len(l) == 3:
        df_1 = to_check_operator(l[0], df_)
        df_2 = to_check_operator(l[1], df_1)
        _df = to_check_operator(l[2], df_2)
        return _df

    elif len(l) == 4:
        df_1 = to_check_operator(l[0], df_)
        df_2 = to_check_operator(l[1], df_1)
        df_3 = to_check_operator(l[2], df_2)
        _df = to_check_operator(l[3], df_3)
        return _df

# creating the flask app 
app = Flask(__name__)
# creating an API object 
api = Api(app)

class final_API(Resource):

    def post(self):
        #accepting POST requests
        json_data = request.get_json()
        #getting query type
        query_type = parse_query_type(json_data)
        #parsing json data 
        req = parse_request(json_data)
        # getting dataframe after applying all the filters from post request.
        final_data = check_length(req)

        if query_type == "discounted_products_list":
            #getting id of all discounted product items.
            discounted_products_list = final_data['_id'].apply(lambda x: x['$oid'])
            return jsonify({'discounted_products_list': list(discounted_products_list)})

        elif query_type == "discounted_products_count|avg_discount":
            #calculating discounted_products_count
            discounted_products_count = int(final_data['discount'].count())
            avg_discount = round(final_data['discount'].mean(),2)
            return jsonify({'discounted_products_count': discounted_products_count, 'avg_discount':avg_discount})

        elif query_type == "expensive_list":
            expensive_list = list(final_data['_id'].apply(lambda x: x['$oid']))
            return jsonify({'expensive_list':expensive_list})

        elif query_type == "competition_discount_diff_list":
            competition_discount_diff_list = list(final_data['_id'].apply(lambda x: x['$oid']))
            return jsonify({'competition_discount_diff_list': competition_discount_diff_list})

# adding the defined resources along with their corresponding urls 
api.add_resource(final_API,'/final_API')

# driver function 
if __name__ == '__main__':
	app.run(debug = True)
