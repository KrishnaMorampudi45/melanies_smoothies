# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col


# Write directly to the app
st.title(":cup_with_straw: Pending Orders :cup_with_straw: ")
st.write(
  """Current pending orders
  """
)
cnx=st.connection('snowflake')
session=cnx.session()
session = get_active_session()
my_dataframe = session.table("smoothies.public.orders").filter(col('ORDER_FILLED')==False).collect()
if my_dataframe:
    editable_df = st.data_editor(my_dataframe)
    #st.dataframe(data=my_dataframe, use_container_width=True)
    
    submitted=st.button('Submit')
    if submitted:
        
        og_dataset = session.table("smoothies.public.orders")
        edited_dataset = session.create_dataframe(editable_df)
        try:
            og_dataset.merge(edited_dataset
                             , (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID'])
                             , [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
                            )
            st.success('Order(s) updated',icon="👍")
        except:
            st.warning('OOPS something went wrong', icon='🤭')
else:
    st.success('No pending orders', icon='🤷‍♂️')
