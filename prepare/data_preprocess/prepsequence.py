import re
from io import StringIO
import pandas as pd
from utils.miscutils import isscalar
from abs.doc.enumerators import node_type_dict, money_source_dict, cond_pairs, limit_pairs, target_pairs


def transform_sequence(data, pre_sec_levels: list):
    """
    支付顺序表格式的处理，但是字段是否正确维护仍然需要在现金流瀑布中判断

    1. 进行一些检查，例如节点编号和父节点编号不能相等、至少 n-1 个节点都需要有父节点、一个节点的父节点编号不能多于两个、分支条件不能含有特殊字符串。
    2. 一些形式上的处理。

    Args:
        data (pd.DataFrame):  原始的支付顺序表
        pre_sec_levels (list): 所有非次级证券的等级

    Returns:
        pd.DataFrame, 处理完的现金流归集表

    """
    data.loc[:, 'node_type'] = data['node_type'].apply(lambda x: node_type_dict[str(x)])

    s = StringIO()
    data.to_csv(s, sep='\t')
    contents = s.getvalue()
    new_contents = contents.lower().replace(" ", "")
    new_contents = new_contents.lower().replace("，", ",").replace("（", "(").replace("）", ")")

    new_file = StringIO(new_contents)
    # node_id 和 parent_node_id 读成字符串，防止变成 float
    data = pd.read_csv(new_file, sep='\t', dtype={'node_no': str, 'parent_node_no': str})

    all_else_clear = "and(" + ",".join(str(i) + ".clear" for i in pre_sec_levels) + ")"  # 优先级是否全部偿还完毕，作为次级超额收益支付的条件

    for idx in data.index:

        # 1. 节点条件
        node_cond = data.loc[idx, "node_condition"]
        if str(node_cond) != 'nan':
            node_cond = str(node_cond).strip(',')
            # 若节点条件不符合规则（例如：是中文；含百分号），则删除该条件
            if (re.fullmatch(r'[,.a-z0-9()\[\]_:]+', node_cond) is None):
                data.loc[idx, "node_condition"] = float('nan')

            else:
                for pair in cond_pairs:
                    node_cond = node_cond.replace(pair[0], pair[1])
                data.loc[idx, "node_condition"] = node_cond

        # 2. 分支条件
        branch_cond = data.loc[idx, "branch_condition"]
        if str(branch_cond) != 'nan':
            branch_cond = str(branch_cond).strip(',')
            if re.fullmatch(r'[,.a-z0-9()\[\]_:]+', branch_cond) is not None:

                for pair in cond_pairs:
                    branch_cond = branch_cond.replace(pair[0], pair[1])
                    data.loc[idx, "branch_condition"] = branch_cond
            else:
                data.loc[idx, "branch_condition"] = False # 直接让这个节点无法进入，程序能够正常运行，但是仍会输出报错要求修改

        # 3. 资金来源
        source = money_source_dict[str(int(data.loc[idx, "money_source"]))] \
            if str(data.loc[idx, "money_source"]) not in ('nan', 'None') else float('nan')
        data.loc[idx, "money_source"] = source

        # 4. 支付上限
        upper_limit = str(data.loc[idx, "upper_limit"])
        if str(upper_limit) != 'nan':
            upper_limit = str(upper_limit).strip(',')

            if re.findall(r'(\.prin)(?!_due)', upper_limit):  # 之前维护的prin应该维护成balance, 否则会跟prin_due混淆，特殊处理下
                upper_limit = upper_limit.replace('.prin', '.balance')
            # 若支付上限不符合规则（例如：是中文；含百分号），则删除该上限
            # 允许含括号，不允许小于10的数字（维护错误，猜测某账户的百分之多少只维护了百分之多少，没有维护对应的账户）
            if re.fullmatch(r'[,.a-z0-9()*+-]+', upper_limit) is None:
                data.loc[idx, "upper_limit"] = float('nan')

            elif re.fullmatch(r'[.0-9]', upper_limit):
                if float(upper_limit) < 10:
                    data.loc[idx, "upper_limit"] = float('nan')

            else:
                for pair in limit_pairs:
                    upper_limit = upper_limit.replace(pair[0], pair[1])
                data.loc[idx, "upper_limit"] = upper_limit

        # 5. 资金去向
        # a1.def_int 之类删掉,不区分是否应付未付，统一记在应付利息里
        target = data.loc[idx, "money_destination"]
        if str(target) != 'nan':
            target = str(target).strip(',')

            for pair in target_pairs:
                target = target.replace(pair[0], pair[1])

            target = target.replace('res', 'ignore')

            data.loc[idx, "money_destination"] = target
            #
            if ('exret' in target) and str(data.loc[idx, 'node_condition']) == 'nan' and \
                    str(data.loc[idx, 'upper_limit']) == 'nan': #强制只要支付次级超额收益,必定要求先还完优先级在前面的证券
                data.loc[idx, "node_condition"] = all_else_clear

            if ('incentive_fee' in target) and str(data.loc[idx, 'upper_limit']) == 'nan': #强制超额激励在没有支付上限时设置上下限为0
                data.loc[idx, "upper_limit"] = 0

        # 6. 如果该节点有两个父节点，就把该行变成两行
        parent_node_id = str(data.loc[idx, "parent_node_no"])
        if parent_node_id.count(',') == 1:
            pnode1, pnode2 = parent_node_id.split(',')
            data.loc[idx, "parent_node_no"] = pnode1
            new_row = data.loc[idx, :].copy(deep=True)
            new_row['parent_node_no'] = pnode2
            data = data.append(new_row)

    # 如果去向和资金来源只有一个有，则报错
    if not (data['money_destination'].isna() == data['money_source'].isna()).all():
        raise ValueError("支付顺序中去向和资金来源不匹配")

    # 按节点编号排序
    data.loc[:, 'node_no_numeric'] = data['node_no'].apply(lambda x: int(x) if isscalar(x) else x)
    data.sort_values(by='node_no_numeric', inplace=True)
    data.drop('node_no_numeric', axis=1, inplace=True)

    return data
