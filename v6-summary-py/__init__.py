import sys
import pandas as pd
import time
import numpy as np
import json

from vantage6.tools.util import warn, info


def master(client, data, *args, **kwargs):
    """
    Master algorithm to compute a summary of the federated datasets.

    Parameters
    ----------
    client : ContainerClient
        Interface to the central server. This is supplied by the wrapper.
    data : dataframe
        Pandas dataframe. This is supplied by the wrapper / node.
    columns : Dictonairy
        Dict containing column names and types

    Returns
    -------
    Dict
        A dictonairy containing summary statistics for all the columns of the
        dataset.
    """
    # define the input for the summary algorithm
    info("Defining input paramaeters")
    input_ = {
        "method": "summary",
        "args": [],
        "kwargs": kwargs
    }

    # obtain organizations that are within my collaboration
    organizations = client.get_organizations_in_my_collaboration()
    ids = [organization.get("id") for organization in organizations]

    # collaboration and image is stored in the key, so we do not need
    # to specify these
    info("Creating node tasks")
    task = client.create_new_task(
        input_,
        organization_ids=ids
    )

    # wait for all results
    # TODO subscribe to websocket, to avoid polling
    task_id = task.get("id")
    # task = client.request(f"task/{task_id}")
    task = client.get_task(task_id) # MOCK
    while not task.get("complete"):
        # task = client.request(f"task/{task_id}")
        task = client.get_task(task_id) # MOCK
        info("Waiting for results")
        time.sleep(1)

    info("Obtaining results")
    results = client.get_results(task_id=task.get("id"))

    info("Check that all nodes have the same columns")
    for res in results:
        for key, colType in res['columns'].items():
            for res2 in results:
                if key not in res2['columns']:
                    warn("Column names are not identical on all sites?!")
                    return None
                if colType != res2['columns'][key]:
                    warn("Column types don't seem to match across sites!")
                    return None

    # process the output
    info("Process node info to global stats")
    g_stats = {}

    # count the total number of rows of all datasets
    info("Count the total number of all rows from all datasets")
    g_stats["number_of_rows"] = sum([x["number_of_rows"] for x in results])

    # remove any results with fewer than 10 rows
    for i, small in reversed(list(enumerate([x["number_of_rows"] < 10 for x in results]))):
        if small:
            info('got a result with fewer than 10 rows, removing it for merging of results')
            results.pop(i)


    # info(f"n={g_stats['number_of_rows']}")

    # compute global statistics for numeric columns
    info("Computing numerical column statistics")
    for colname in results[0]['statistics'].keys():
        if not results[0]['columns'][colname] == 'numeric':
            continue

        n = g_stats["number_of_rows"]

        # extract the statistics for each column and all results
        stats = [result["statistics"][colname] for result in results]

        # compute globals
        g_min = min([x.get("min") for x in stats])
        # info(f"g_min={g_min}")
        g_max = max([x.get("max") for x in stats])
        # info(f"g_max={g_max}")
        g_nan = sum([x.get("nan") for x in stats])
        # info(f"g_nan={g_nan}")
        g_mean = sum([x.get("sum") for x in stats]) / (n-g_nan)
        # info(f"g_mean={g_mean}")
        g_std = (sum([x.get("sq_dev_sum") for x in stats]) / (n-1-g_nan))**(0.5)

        # estimate the median
        # see https://stats.stackexchange.com/questions/103919/estimate-median-from-mean-std-dev-and-or-range
        u_std = (((n-1)/n)**(0.5)) * g_std
        g_median = [
            max([g_min, g_mean - u_std]),
            min([g_max, g_mean + u_std])
        ]

        g_stats[colname] = {
            "min": g_min,
            "max": g_max,
            "nan": g_nan,
            "mean": g_mean,
            "std": g_std,
            "median": g_median
        }

    # compute global statistics for categorical columns
    info("Computing categorical column statistics")
    for colname in results[0]['statistics'].keys():
        if results[0]['columns'][colname] != 'categorical':
            continue
        
        stats = [result["statistics"][colname]['counts'] for result in results]
        all_keys = list(set([key for result in results for key in result["statistics"][colname]['counts'].keys()]))
        
        categories_dict = dict()
        for key in all_keys:
            key_sum = sum([x.get(key) for x in stats if key in x.keys()])
            categories_dict[key] = key_sum

        cat_stats = {'categories': categories_dict}
        cat_stats['nan'] = sum([result['statistics'][colname]['nan'] for result in results])
        g_stats[colname] = cat_stats

        g_stats

    info("master algorithm complete")

    return g_stats

def RPC_summary(dataframe):
    """
    Computes a summary of all columns of the dataframe

    Parameters
    ----------
    dataframe : pandas dataframe
        Pandas dataframe that contains the local data.

    Returns
    -------
    Dict
        A Dict containing some simple statistics for the local dataset.
    """

    # create series from input column names
    info('List of numeric columns:')
    numeric_columns = list(dataframe.select_dtypes(include=[np.number]).columns)
    info(str(numeric_columns))

    print('List of non-numeric columns:')
    non_numerics = list(dataframe.select_dtypes(exclude=[np.number]).columns)

    columns = {
        **{colname: 'numeric' for colname in numeric_columns},
        **{colname: 'categorical' for colname in non_numerics}    
    }
    
    # count the number of rows in the dataset
    info("Counting number of rows")
    number_of_rows = len(dataframe)
    if number_of_rows < 10:
        warn("Dataset has less than 10 rows. Exiting.")
        return {
            "number_of_rows": number_of_rows,
            "columns": columns,
        }

    # min, max, median, average, Q1, Q3, missing_values
    statistics = {}
    for column_name in numeric_columns:
        info(f"Numerical column={column_name} is processed")
        column_values = dataframe[column_name]
        q1, median, q3 = column_values.quantile([0.25,0.5,0.75]).values
        mean = column_values.mean()
        minimum = column_values.min()
        maximum = column_values.max()
        nan = column_values.isna().sum()
        total = column_values.sum()
        std = column_values.std()
        sq_dev_sum = (column_values-mean).pow(2).sum()
        statistics[column_name] = {
            "min": minimum,
            "q1": q1,
            "median": median,
            "mean": mean,
            "q3": q3,
            "max": maximum,
            "nan": int(nan),
            "sum": total,
            "sq_dev_sum": sq_dev_sum,
            "std": std
        }

    # return the categories in categorial columns
    for column_name in non_numerics:
        info(f"Categorical column={column_name} is processed")
        statistics[column_name] = {
            'counts': dataframe[column_name].value_counts().to_dict(),
            'nan': dataframe[column_name].isna().sum()
        }

    return {
        "number_of_rows": number_of_rows,
        "statistics": statistics,
        "columns": columns
    }