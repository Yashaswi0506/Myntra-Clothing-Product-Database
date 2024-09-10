#!/usr/bin/env python
# coding: utf-8

# In[2]:


import psycopg2
import pandas as pd

def initialize():
    connection = psycopg2.connect(
        user = "postgres", #username that you use
        password = "*****", #password that you use, you don't need to include your password when submiting your code
        host = "localhost", 
        port = "5432", 
        database = "postgres"
    )
    connection.autocommit = True
    return connection

def runQuery(conn,number):
    if number ==1:
        select_Query1= "Select distinct(s.size_option) from size s, products p where p.product_id = s.product_id and                    p.product_id in                     (Select product_id from products where p.brand_name = 'Forever New'                    and p.original_price =                     (Select max(original_price) from products where brand_name = 'Forever New'));"
        size_df = pd.DataFrame(columns = ['size_option'])
        with conn.cursor() as cursor:
            cursor.execute(select_Query1)
            records = cursor.fetchall()
            for row in records:
                output_df = {'size_option': row[0]}
                size_df = size_df.append(output_df, ignore_index=True)
            pd.get_option("display.max_columns")
            print("total rows retrived:"+str(len(size_df)))
            print("displaying first 5 rows")
            print(size_df.head(5))
                
    if number ==2:
        select_Query2= "Select c.customer_id, c.customer_name from customers c                     where c.customer_id in (Select distinct(customer_id) from orders                     group by customer_id having count(customer_id) = 1)"
        size_df = pd.DataFrame(columns = ['customer_id', 'customer_name'])
        with conn.cursor() as cursor:
            cursor.execute(select_Query2)
            records = cursor.fetchall()
            for row in records:
                output_df = {'customer_id': row[0], 'customer_name': row[1]}
                size_df = size_df.append(output_df, ignore_index=True)
            #pd.get_option("display.max_columns")
            pd.set_option('display.max_columns', None)
            print("total rows retrived:"+str(len(size_df)))
            print("displaying first 5 rows")
            print(size_df.head(5))
            
    if number ==3:
        select_Query3= "Select p.product_id, p.description, p.discounted_price, s.size_option, p.ratings                         from products p JOIN size s on p.product_id = s.product_id                         JOIN individualcategory i on p.individual_category = i.ind_id                         where i.sub_category = 'sweaters' and s.size_option = 'M'                        and p.gender='Women'"
        size_df = pd.DataFrame(columns = ['p.product_id','p.description','p.discounted_price','s.size_option','p.ratings'])
        with conn.cursor() as cursor:
            cursor.execute(select_Query3)
            records = cursor.fetchall()
            for row in records:
                output_df = {'p.product_id': row[0],'p.description': row[1],'p.discounted_price':row[2],'s.size_option': row[3],'p.ratings':row[4]}
                size_df = size_df.append(output_df, ignore_index=True)
            #pd.get_option("display.max_columns")
           # pd.set_option('display.max_columns', None)
            pd.set_option('display.max_rows', None)
            pd.options.display.max_columns = None
            print("total rows retrived:"+str(len(size_df)))
            print("displaying first 5 rows")
            print(size_df.head(5))
            
            
                
    if number ==4:
        select_Query4= "Select p.product_id , p.description from                         products p join orderproducts o on p.product_id = o.product_id                         join orders od on o.order_id = od.order_id                         where od.customer_id in                         (Select customer_id from customers                        where age =20);"
        size_df = pd.DataFrame(columns = ['p.product_id','p.description'])
        with conn.cursor() as cursor:
            cursor.execute(select_Query4)
            records = cursor.fetchall()
            for row in records:
                output_df = {'p.product_id': row[0],'p.description': row[1]}
                size_df = size_df.append(output_df, ignore_index=True)
            pd.get_option("display.max_columns")
            print("total rows retrived:"+str(len(size_df)))
            print("displaying first 5 rows")
            print(size_df.head(5))
                    
    if number ==5:
        select_Query5= "Select  distinct(count(order_id)) as order_count from                         orderproducts                         where product_id IN(                         Select product_id from products where ratings IS NULL and reviews IS NULL);"
        size_df = pd.DataFrame(columns = ['order_count'])
        with conn.cursor() as cursor:
            cursor.execute(select_Query5)
            records = cursor.fetchall()
            for row in records:
                output_df = {'order_count': row[0]}
                size_df = size_df.append(output_df, ignore_index=True)
            pd.get_option("display.max_columns")
            print("total rows retrived:"+str(len(size_df)))
            print("displaying first 5 rows")
            print(size_df.head(5))

    if number == 6:
        select_Query6= "Select p.product_id, p.description, p.discounted_price , p.brand_name         from products p JOIN individualcategory i  on p.individual_category = i.ind_id         where i.category = 'Sports Wear' and i.sub_category = 'tshirts';" 
        size_df = pd.DataFrame(columns = ['product_id','description','discounted_price','brand_name' ])
        with conn.cursor() as cursor:
            cursor.execute(select_Query6)
            records = cursor.fetchall()
            for row in records:
                output_df = {'product_id' : row[0],'description' : row[1],'discounted_price': row[2], 'brand_name': row[3]}
                size_df = size_df.append(output_df, ignore_index=True)
            pd.get_option("display.max_columns")
            print("total rows retrived:"+str(len(size_df)))
            print("displaying first 5 rows")
            print(size_df.head(5))
            
    if number == 7:
        select_Query7= "Select p.product_id , i.category from individualcategory i                         Join products p on i.ind_id = p.individual_category                         Join orderproducts o on p.product_id = o.product_id and o.order_id = 15;"

        size_df = pd.DataFrame(columns = ['product_id','category' ])
        with conn.cursor() as cursor:
            cursor.execute(select_Query7)
            records = cursor.fetchall()
            for row in records:
                output_df = {'product_id' : row[0],'category' : row[1]}
                size_df = size_df.append(output_df, ignore_index=True)
            pd.get_option("display.max_columns")
            print("total rows retrived:"+str(len(size_df)))
            print("displaying first 5 rows")
            print(size_df.head(5))
            
    if number == 8:
        select_Query8= "Select distinct(o1.product_id) from customers c                         JOIN orders o ON c.customer_id = o.customer_id                         Join orderproducts o1 on o.order_id = o1.order_id and c.age > 60  "

        size_df = pd.DataFrame(columns = ['product_id' ])
        with conn.cursor() as cursor:
            cursor.execute(select_Query8)
            records = cursor.fetchall()
            for row in records:
                output_df = {'product_id':row[0]}
                size_df = size_df.append(output_df, ignore_index=True)
            pd.get_option("display.max_columns")
            print("total rows retrived:"+str(len(size_df)))
            print("displaying first 5 rows")
            print(size_df.head(5))
      
    if number == 9:
        select_Query9= "Select brand_name , count(p.brand_name) as count_of_bottom_wear                          from products p JOIN individualcategory i on p.individual_category = i.ind_id                         JOIN size s on p.product_id = s.product_id and s.size_option ='28'and                         i.category = 'Bottom Wear' group by(brand_name);"


        size_df = pd.DataFrame(columns = ['brand_name', 'count_of_bottom_wear' ])
        with conn.cursor() as cursor:
            cursor.execute(select_Query9)
            records = cursor.fetchall()
            for row in records:
                output_df = {'brand_name' : row[0], 'count_of_bottom_wear' : row[1]}
                size_df = size_df.append(output_df, ignore_index=True)
            pd.get_option("display.max_columns")
            print("total rows retrived:"+str(len(size_df)))
            print("displaying first 5 rows")
            print(size_df.head(5))
    
    if number == 10:
        select_Query10= "Select * from                         (Select o.product_id, sum(quantity) as t1 , p.brand_name ,                         DENSE_RANK() OVER(ORDER BY sum(quantity) desc ) Rank                         from orderproducts o  JOIN products p ON o.product_id = p.product_id                         group by (o.product_id,p.brand_name) having sum(quantity)in(Select  total  from                           (Select  sum(quantity) as total from orderproducts                          group by product_id ) as a order by total desc)  order by t1 desc)as f where Rank <6 ;"

        size_df = pd.DataFrame(columns = ['product_id','quantity','brand_name','Rank' ])
        with conn.cursor() as cursor:
            cursor.execute(select_Query10)
            records = cursor.fetchall()
            for row in records:
                output_df = {'product_id' : row[0], 'quantity' : row[1], 'brand_name' : row[2], 'Rank' : row[3]}
                size_df = size_df.append(output_df, ignore_index=True)
            pd.get_option("display.max_columns")
            print("total rows retrived:"+str(len(size_df)))
            print("displaying first 6 rows")
            print(size_df.head(6))


             


def main():
    number = int(input("Enter the query number: "))
    conn = initialize()
    runQuery(conn,number)
    conn.close()


if __name__ == "__main__":
    main()


    


# In[ ]:





# In[ ]:




