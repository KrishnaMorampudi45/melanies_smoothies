# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col
import requests
smoothiefroot_response = requests.get("https://my.smoothiefroot.com/api/fruit/watermelon")
#st.text(smoothiefroot_response)
sf_df=st.dataframe(data=smoothiefroot_response.json(),use_container_width=True)
# Write directly to the app
st.title(":cup_with_straw: Customize your Smoothie :cup_with_straw: ")
st.write(
  """Customize your own Smoothie
  """
)
name_on_order=st.text_input("Name on Smoothie")
st.write('name on the smoothie will be',name_on_order)
cnx=st.connection('snowflake')
session=cnx.session()
my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))
#st.dataframe(data=my_dataframe, use_container_width=True)
ingredient_list=st.multiselect('Choose upto 5 ingredients:',my_dataframe,max_selections=5)
if ingredient_list:
    
    ingredients_string=''
    for i in ingredient_list:
        ingredients_string+=i+' '
    st.write(ingredients_string)
    my_insert_stmt = """ insert into smoothies.public.orders(ingredients,name_on_smoothie)
            values ('""" + ingredients_string + """','""" + name_on_order + """')"""
    insert=st.button('Submit Order')
    #st.write(my_insert_stmt)
    #st.stop()
    if insert:
        session.sql(my_insert_stmt).collect()
        st.success(f'Your Smoothie is ordered!, Thankyou {name_on_order}', icon="✅")
    
