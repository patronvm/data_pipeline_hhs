import os
import sys


'''
This module runs the dashboard via streamlit
'''


date = sys.argv[1]
os.system("streamlit run dashboard.py {}".format(date))
