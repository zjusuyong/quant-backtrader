import datetime as dt

"""全局配置文件"""


HR_POSITION_TYPE = {}
HR_POSITION_LEVEL = {}
HR_LEADER_LEVEL = []
HR_LEVEL_VAL = {}
HR_ORIGINAL_COL = []
HR_ASCEND_DIM_COL = []
HR_ANALYSIS_COL = []
LIST_CNST_STH_TO_STR = []
PERF_NDXLIB_TYPE_DICT = {}
COST_ORIGINAL_COL = []
COST_ASCEND_DIM_COL = []
COST_PIVOT_COL = []
COST_ANALYSIS_COL = []

"""WorkDay"""
# 要增加的工作日
ADD_WORKDATE = [dt.date(2019, 9, 29),
                dt.date(2019, 10, 12)]

# 要去掉的工作日
DROP_WORKDATE = [dt.date(2019, 9, 13),
                 dt.date(2019, 10, 1),
                 dt.date(2019, 10, 2),
                 dt.date(2019, 10, 3),
                 dt.date(2019, 10, 4),
                 dt.date(2019, 10, 7)]


