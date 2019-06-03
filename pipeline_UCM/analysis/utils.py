"""
Utility functions for analyses and plotting
for HIV retention for UChicago Clinic
"""

import yaml
import os
import pandas as pd
import numpy as np
import math
import logging
import seaborn as sns
from sklearn import metrics

from sqlalchemy import create_engine
import matplotlib.pyplot as plt

from triage.component.audition import Auditioner
from aequitas.group import Group


# connect to db
db_url = os.environ['DBURL']
engine = create_engine(db_url)


def get_best_model_group_from_list(model_groups):
    if len(model_groups) == 0:
        return(None)
    if len(model_groups) == 1:
        return({'most_frequent_best_dist_precision@_10.0_pct_0.05':
                model_groups})
    sel = """ SELECT DISTINCT train_end_time
              FROM model_metadata.models
              WHERE model_group_id IN ({})
              AND extract(year from train_end_time) <= 2015 ;
    """.format(', '.join([str(x) for x in model_groups]))
    end_times = sorted(list(pd.read_sql(sel, engine)['train_end_time']))
    # print(len(model_groups))
    # print(end_times)
    aud = Auditioner(
        db_engine=engine,
        model_group_ids=model_groups,
        train_end_times=end_times,
        initial_metric_filters=[{'metric': 'precision@',
                                 'parameter': '10.0_pct',
                                 'max_from_best': 1.0,
                                 'threshold_value': 0.0,
                                 }],
        models_table='models',
        distance_table='kr_test_dist'
    )
    seln_rules = [{
        'shared_parameters':
        [{'metric': 'precision@', 'parameter': '10.0_pct'}],
        'selection_rules':
        [{'name': 'most_frequent_best_dist', 'dist_from_best_case': [0.05]}]
    }]

    aud.register_selection_rule_grid(seln_rules, plot=False)
    best_model_group = aud.selection_rule_model_group_ids
    return(best_model_group)


def plot_pr_at_k_for_model(model_id, title):
    query = f"""select score, label_value
        from test_results.predictions
        where model_id = {model_id};"""
    df_scores = pd.read_sql(query, engine)
    y_true = df_scores.label_value.values
    y_score = df_scores.score.values

    query = f"""select model_type,
                  extract(year from train_end_time)::int as train_end_year
        from model_metadata.models
        where model_id = {model_id};"""
    model_info = pd.read_sql(query, engine)
    fig_title = model_info['model_type'][0].split('.')[2] + \
        ' trained on data from before ' + str(model_info['train_end_year'][0])

    sns.set_style('whitegrid')
    sns.set_context("poster", font_scale=1, rc={
                    "lines.linewidth": 2.25, "lines.markersize": 10})

    precision_curve, recall_curve, pr_thresholds = \
        metrics.precision_recall_curve(y_true, y_score)
    precision_curve = precision_curve[:-1]
    recall_curve = recall_curve[:-1]
    pr_thresholds = np.insert(pr_thresholds, 0, 0)
    precision_curve = np.insert(precision_curve, 0, precision_curve[0])
    recall_curve = np.insert(recall_curve, 0, recall_curve[0])

    pct_above_per_thresh = []
    number_scored = len(y_score)
    m = None
    for i, value in enumerate(pr_thresholds):
        num_above_thresh = len(y_score[y_score >= value])
        pct_above_thresh = num_above_thresh / float(number_scored)
        if not m and pct_above_thresh < 0.10:
            m = i-1
        pct_above_per_thresh.append(pct_above_thresh)
    pct_above_per_thresh = np.array(pct_above_per_thresh)
    plt.clf()
    fig, ax1 = plt.subplots(figsize=(8, 6))
    fontsize = 20
    ax1.axvline(0.1, linestyle='--', color='dimgray')
    ax1.plot(pct_above_per_thresh, precision_curve, "#000099")
    ax1.plot(pct_above_per_thresh[m],
             precision_curve[m], "#000099", marker='o')
    ax1.set_xlabel('Proportion of population', fontsize=fontsize)
    ax1.set_ylabel('Positive Predictive Value',
                   color="#000099", fontsize=fontsize)
    plt.ylim([0.0, 1.05])
    ax2 = ax1.twinx()
    ax2.plot(pct_above_per_thresh, recall_curve, "#CC0000")
    ax2.plot(pct_above_per_thresh[m], recall_curve[m], "#CC0000", marker='o')
    ax2.set_ylabel('Sensitivity', color="#CC0000", fontsize=fontsize)
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.title(title)
    plt.show()


def get_prettified_feature_group(triage_feature):
    if "imp" in triage_feature:
        return("Previous ID Encounters")
    elif "cd4" in triage_feature:
        return ("CD4 Count")
    elif "prevappts" in triage_feature:
        return("Previous ID Encounters")
    elif "idprevappts" in triage_feature:
        return("Previous (non-ID) Encounters")
    elif "retention" in triage_feature:
        return("Retention History")
    elif "providers" in triage_feature:
        return("Providers")
    elif "vl" in triage_feature:
        return("Viral Load")
    elif "demo" in triage_feature:
        return("Demographics")
    elif "diag" in triage_feature:
        return("Diagnoses")
    elif "crime" in triage_feature:
        return("Crime")
    elif "acs" in triage_feature:
        return("ACS")
    elif "loc" in triage_feature:
        return("Location")
    elif "insurance" in triage_feature:
        return("Insurance")
    elif "first_appt" in triage_feature:
        return("First Appointment")
    elif "expert" in triage_feature:
        return("Expert")
    elif "fac" in triage_feature:
        return("Facilities")
    elif "hospital" in triage_feature:
        return("Hospitalization")
    elif "med" in triage_feature:
        return("Medications")
    else:
        print(triage_feature)
        return("Other")


def make_pretty(triage_feature):
    s = ""
    if "n_days_last_appt" in triage_feature:
        return("Number of days since last appointment")
    if "avg" in triage_feature:
        s = "Average"
    elif "max" in triage_feature:
        s = "Max"
    elif "min" in triage_feature:
        s = "Min"
    elif "min" in triage_feature:
        s = "Average"
    elif "sum" in triage_feature:
        s = "Number of"
    if "days_bn" in triage_feature:
        s += " days between"
    fg = get_prettified_feature_group(triage_feature)
    s += " " + fg
    if "6" in triage_feature:
        s += " in last 6 months"
    elif "1year" in triage_feature:
        s += " in last year"
    elif "3year" in triage_feature:
        s += " in last 3 years"
    elif "all" in triage_feature:
        s += " over all time"
    return(s)


def plot_feature_imp(model_id, func, title):
    q = f'''select * from
    train_results.feature_importances
    where model_id = {model_id}
    and feature_importance > 0
    order by feature_importance desc;
    '''
    df_feat = pd.read_sql(q, engine)
    df_feat['fg'] = df_feat['feature'].apply(
        lambda x: get_prettified_feature_group(x))
    df_feat['f'] = df_feat['feature'].apply(lambda x: make_pretty(x))

    c = df_feat.groupby('fg')['feature_importance'].agg({'val': func})
    col = 'val'
    c = c.sort_values(col, ascending=False).reset_index()
    sns.set_style('whitegrid')
    sns.set_context("poster", font_scale=1)
    fig, ax = plt.subplots(1, 1, figsize=(22, 8))
    N = c.shape[0]
    ind = np.arange(N)
    ax.bar(ind, c[col], color='gray', align="center")
    _ = plt.xticks(ind, c.fg)
    plt.ylabel("Maximum predictor importance" +
               "\nby group in machine learning model for \n"+title)
    plt.xticks(rotation=90)
    plt.show()


def get_model_evaluation(model_group_id,
                         metric, parameter,
                         test_or_train='test', filter_x=None):
    logging.debug('model_group_id: {} metric: {} parameter {}'.format(
        model_group_id, metric, parameter))
    """
    Grab model evaluation

    """
    if test_or_train == 'test':
        q = f"""select
        evaluation_start_time,
        value
        from test_results.evaluations
        where
            model_id in (select distinct model_id
                         from model_metadata.models
                         where model_group_id = {model_group_id})
            and metric = '{metric}'
            and parameter = '{parameter}'
        order by evaluation_end_time;
        """
        dfx = pd.read_sql(q, engine, parse_dates=['evaluation_start_time'])

    else:
        q = f"""select
        evaluation_end_time,
        value
        from train_results.evaluations
        where
            model_id in (select distinct model_id
                         from model_metadata.models
                         where model_group_id = {model_group_id})
            and metric = '{metric}'
            and parameter = '{parameter}'
        order by evaluation_end_time;
            """
        dfx = pd.read_sql(q, engine, parse_dates=['evaluation_end_time'])

    logging.debug(q)
    if dfx.empty:
        raise ValueError('no rows in data frame for this metric and value')
    logging.debug(dfx.head())
    x, y = zip(*dfx.values.tolist())

    if filter_x:
        xy = [xi for xi in zip(x, y) if filter_x(xi)]
        return zip(*xy)
    else:
        return x, y


def get_model_id_at_time(model_group_id, time):
    q = f"""
    select *
    from model_metadata.models
    where model_group_id = {model_group_id}
    and train_end_time = \'{time}\'
    """
    model_id = pd.read_sql(q, engine)['model_id'][0]
    return (model_id)


def get_demographics(model_group_to_use):
    query = f"""with gender_race_info as (
        select distinct entity_id,
            race_id, race
        from features_cs.demographics
        join lookup_ucm.race using (race_id)
        )

        select model_id, p.entity_id, p.as_of_date,
                extract(year from p.as_of_date) as year,
                label_value,
                race
        from test_results.predictions p
        left join gender_race_info using (entity_id)
        join model_metadata.models using (model_id)
        where model_group_id = 20854
        """
    demo = pd.read_sql(query, engine)
    return(demo)


def get_models_same_mg(mg_id):
    q = f"""
        select model_id, train_end_time from model_metadata.models
        where model_group_id = {mg_id}
        and train_end_time < '2016-01-01'
        """
    df = pd.read_sql(q, engine)
    return(df)


def get_predictions(m_id):
    q = f"""
        select model_id, entity_id, as_of_date,
            score
        from test_results.predictions p
        where model_id ={m_id}
        """
    df = pd.read_sql(q, engine)
    return (df)


def get_for_race(m_id, demo, thresholds={'rank_pct': [0.1]}):
    for_race = []
    pred = get_predictions(m_id)
    # merge with demo
    df = pd.merge(pred, demo, on=['entity_id', 'as_of_date'])
    df = df.loc[df.groupby('entity_id')['label_value'].idxmax(), :]
    df = df[["entity_id", "score", "label_value", "race"]]
    g = Group()
    xtab, _ = g.get_crosstabs(df, thresholds)
    for_black = xtab.loc[xtab.attribute_value ==
                         'Black/African-American', 'for'].values[0]
    for_white = xtab.loc[xtab.attribute_value == 'White', 'for'].values[0]
    for_race = for_black/for_white
    return(for_race)


def get_model_evaluation_with_model(mg_id, metric, parameter):
    q = f"""
        select *
        from test_results.evaluations e
        where metric='{metric}' and parameter='{parameter}'
        and model_id in
            (select model_id from model_metadata.models
             where model_group_id={mg_id})
        """
    df = pd.read_sql(q, engine)
    return(df)
