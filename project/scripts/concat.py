import os
import pandas as pd

def concat_files(path, month):
    """
    concat daily data into one dataframe
    :param path: folder path where daily data files exist
    :param month: data month
    :return: dataframe that combines all daily data files
    """
    dataframe = pd.DataFrame()
    for num in range(1, 32):
        file_name = '2022-%s-%s.parquet'%(str(month).zfill(2), str(num).zfill(2))
        try:
            # print(os.path.join(search_data_path, file_name))
            df = pd.read_parquet(os.path.join(path, file_name))
            dataframe = pd.concat([dataframe, df], ignore_index=True)
        except:
            continue
    dataframe = dataframe.drop_duplicates(ignore_index=True)
    return dataframe

def concat_month_to_year(path, month):
    dataframe = pd.DataFrame()
    for i in range(1, month+1):
        file_name = '2022-%s-01.token.nextonly.parquet'%(str(i).zfill(2))
        file_path = os.path.join(path, file_name)
        data = pd.read_parquet(file_path)
        dataframe = pd.concat([dataframe, data])
        for n in range(1, 5):
            file_name_extra = '2022-%s-01.%s.token.nextonly.parquet'%(str(i).zfill(2), str(n))
            file_path_extra = os.path.join(path, file_name_extra)
            if os.path.exists(file_path_extra):
                data = pd.read_parquet(file_path_extra)
                dataframe = pd.concat([dataframe, data])
    dataframe = dataframe.drop_duplicates(ignore_index=True)
    return dataframe

def concat_video(path, batch):
    dataframe = pd.DataFrame()
    for i in range(0, batch+1):
        file_name = '2022-01_2022-10.batch%s.parquet'%(str(i))
        file_path = os.path.join(path, file_name)
        data = pd.read_parquet(file_path)
        dataframe = pd.concat([dataframe, data])
    dataframe = dataframe.drop_duplicates(ignore_index=True)
    return dataframe

def main(folder, file_name, month):
    base_path = '/home/fagabby/working/YoutubeProject/project'
    daily_file_path = os.path.join(base_path, 'db/search_data/daily')
    data = concat_files(month=month, path=daily_file_path)
    file_path = os.path.join(base_path, folder, file_name)
    data.to_parquet(file_path+'.parquet', compression='gzip')


if __name__ == '__main__':
    # concat daily file to monthly
    # file_name = '2022-04-01_to_2022-04-29'
    # month = str(4).zfill(2)
    # main(folder='db/search_data/monthly_data', file_name=file_name, month=month)

    # #concat monthly files to a full year combo
    # path = os.path.abspath(os.path.join(os.getcwd(), os.pardir)) + '/db/search_data/monthly_data'
    # month = 10
    # df = concat_month_to_year(path, month)
    # start_mth = 1
    # end_mth = 10
    # file_name_to_save = '2022-%s_2022-%s.parquet'%(str(start_mth).zfill(2), str(end_mth).zfill(2))
    # save_file = os.path.abspath(os.path.join(os.getcwd(), os.pardir)) + '/db/search_data/' + file_name_to_save
    # df.to_parquet(save_file, compression='gzip')

    #concat video data
    path = os.path.abspath(os.path.join(os.getcwd(), os.pardir)) + '/db/video_data'
    batch = 23
    df = concat_video(path, batch)
    start_mth = 1
    end_mth = 10
    file_name_to_save = '2022-%s_2022-%s.video.parquet' % (str(start_mth).zfill(2), str(end_mth).zfill(2))
    save_file = os.path.abspath(os.path.join(os.getcwd(), os.pardir)) + '/db/video_data/' + file_name_to_save
    print(save_file)
    df.to_parquet(save_file, compression='gzip')