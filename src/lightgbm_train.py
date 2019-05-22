"""
使用lightgbm预测数据
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from baseModel.jfmaTime import MyTime
import os


def pred_by_ligtgbm():
    all_history_base = pd.read_csv("../data/train_data.csv")
    all_history_base = all_history_base.drop(columns=["category_id", "create_time", "people", "put_time_interval"])
    pd_data_train_label = all_history_base["expo_num"]
    pd_data_train_target = all_history_base.drop(columns=["expo_num"])
    pd_data_text_obj = pd.read_csv("../data/test_sample.csv")
    pd_data_text_target = pd_data_text_obj.drop(
        columns=["category_id", "sample_id", "create_time", "people", "put_time_interval"])
    X_train, X_test, y_train, y_test = train_test_split(pd_data_train_target, pd_data_train_label, test_size=0.3,
                                                        random_state=0)
    trn_data = lgb.Dataset(X_train, y_train, categorical_feature=["goods_type", "goods_id", "ad_account_id"])
    test_data = lgb.Dataset(X_test, y_test, reference=True)
    cv_data = lgb.Dataset(pd_data_train_target, pd_data_train_label,
                          categorical_feature=["goods_type", "goods_id", "ad_account_id"], free_raw_data=False)
    params = {'num_leaves': 31,
              'max_depth': 7,
              'learning_rate': 0.01,
              'objective': 'regression',
              'boosting': 'gbdt',
              'bagging_fraction': 0.8,
              # 'feature_fraction': 0.8201
              }
    num_round = 100000
    best_params = {}
    max_auc = float('100000000000')
    print("调参1：提高准确率")
    for num_leaves in range(5, 65, 5):
        for max_depth in range(3, 8, 1):
            # print("num_leaves:{} max_depth:{}".format(num_leaves, max_depth))
            params['num_leaves'] = num_leaves
            params['max_depth'] = max_depth
            mean_auc = use_cv(lgb, params, cv_data)
            if mean_auc < max_auc:
                max_auc = mean_auc
                best_params['num_leaves'] = num_leaves
                best_params['max_depth'] = max_depth
    if 'num_leaves' and 'max_depth' in best_params.keys():
        params['num_leaves'] = best_params['num_leaves']
        params['max_depth'] = best_params['max_depth']
    print("调参2：降低过拟合")
    max_auc = float('100000000000')
    for max_bin in range(5, 256, 10):
        for min_data_in_leaf in range(1, 102, 10):
            params['max_bin'] = max_bin
            params['min_data_in_leaf'] = min_data_in_leaf
            mean_auc = use_cv(lgb, params, cv_data)
            if mean_auc < max_auc:
                max_auc = mean_auc
                best_params['max_bin'] = max_bin
                best_params['min_data_in_leaf'] = min_data_in_leaf
    if 'max_bin' and 'min_data_in_leaf' in best_params.keys():
        params['min_data_in_leaf'] = best_params['min_data_in_leaf']
        params['max_bin'] = best_params['max_bin']
    print("调参3：降低过拟合")
    max_auc = float('100000000000')
    for feature_fraction in [0.6, 0.7, 0.8, 0.9, 1.0]:
        for bagging_fraction in [0.6, 0.7, 0.8, 0.9, 1.0]:
            for bagging_freq in range(0, 50, 5):
                params['feature_fraction'] = feature_fraction
                params['bagging_fraction'] = bagging_fraction
                params['bagging_freq'] = bagging_freq
                mean_auc = use_cv(lgb, params, cv_data)
                if mean_auc < max_auc:
                    max_auc = mean_auc
                    best_params['feature_fraction'] = feature_fraction
                    best_params['bagging_fraction'] = bagging_fraction
                    best_params['bagging_freq'] = bagging_freq
    if 'feature_fraction' and 'bagging_fraction' and 'bagging_freq' in best_params.keys():
        params['feature_fraction'] = best_params['feature_fraction']
        params['bagging_fraction'] = best_params['bagging_fraction']
        params['bagging_freq'] = best_params['bagging_freq']
    # type(X_train.iloc[1,1])
    print("调参4：降低过拟合")
    max_auc = float('100000000000')
    for lambda_l1 in [1e-5, 1e-3, 1e-1, 0.0, 0.1, 0.3, 0.5, 0.7, 0.9, 1.0]:
        for lambda_l2 in [1e-5, 1e-3, 1e-1, 0.0, 0.1, 0.4, 0.6, 0.7, 0.9, 1.0]:
            params['lambda_l1'] = lambda_l1
            params['lambda_l2'] = lambda_l2
            mean_auc = use_cv(lgb, params, cv_data)
            if mean_auc < max_auc:
                max_auc = mean_auc
                best_params['lambda_l1'] = lambda_l1
                best_params['lambda_l2'] = lambda_l2
    if 'lambda_l1' and 'lambda_l2' in best_params.keys():
        params['lambda_l1'] = best_params['lambda_l1']
        params['lambda_l2'] = best_params['lambda_l2']

    print("调参5：降低过拟合2")
    max_auc = float('100000000000')
    for min_split_gain in [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]:
        params['min_split_gain'] = min_split_gain
        mean_auc = use_cv(lgb, params, cv_data)
        if mean_auc < max_auc:
            max_auc = mean_auc
            best_params['min_split_gain'] = min_split_gain
    if 'min_split_gain' in best_params.keys():
        params['min_split_gain'] = best_params['min_split_gain']

    print(best_params)
    print(params)

    clf = lgb.train(params, trn_data, num_round, valid_sets=[trn_data, test_data], verbose_eval=1000,
                    early_stopping_rounds=100)
    print(clf.best_iteration)
    ypred = clf.predict(pd_data_text_target, num_iteration=clf.best_iteration)
    get_result(pd_data_text_obj, ypred)


def use_cv(lgb, params, cv_data):
    cv_results = lgb.cv(
        params,
        cv_data,
        seed=1,
        nfold=5,
        metrics=['l2_root'],
        early_stopping_rounds=100,
        verbose_eval=None
    )
    mean_auc = pd.Series(cv_results['rmse-mean']).min()
    return mean_auc


def get_result(ad_pd, ypred):
    """
    将计算结果导入到一个csv文件中
    :param ad_pd: 此为一个预测广告
    :param ypred: 此为预测值
    :return:
    """
    pd_id = ad_pd.loc[:, ["sample_id"]]
    df = pd.DataFrame(ypred, columns=["value"])
    z = pd.concat([pd_id, df], axis=1)
    z.loc[z["value"] < 0, "value"] = 0
    z["value"] = z["value"].round(decimals=4)
    z.to_csv("..{0}result{0}submission_{1}.csv".format(os.path.sep, MyTime.get_loc_time_str(MyTime.time_pattern4))
             , header=0, index=0)


if __name__ == '__main__':
    pred_by_ligtgbm()
