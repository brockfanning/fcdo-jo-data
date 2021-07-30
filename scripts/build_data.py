from sdg.open_sdg import open_sdg_build
from sdg import helpers


def fix_time_period(x):
    x = str(x)
    years = x.split('-')
    return str(int(years[0]))


def set_time_detail(df):
    if 'TIME_DETAIL' not in df.columns.to_list():
        df = df.copy()
        df['TIME_DETAIL'] = df['Year']
    df['Year'] = df['Year'].apply(fix_time_period)
    return df


def columns_to_drop():
    return [
        'scale',
        'objectivo',
        'meta',
        'indicador'
    ]


def common_alterations(df):
    df['REPORTING_TYPE'] = 'N'
    return df


def drop_columns(df):
    columns_in_data = df.columns.to_list()
    for column in columns_to_drop():
        if column in columns_in_data:
            df = df.drop([column], axis=1)
    return df


def limit_to_national_ref_area(df, ref_area):
    def row_matches_ref_area(row):
        return row['REF_AREA'] == ref_area

    df = df.copy()
    mask = df.apply(row_matches_ref_area, axis=1)
    return df[mask]


def set_series_and_unit(df, context):
    if 'SERIES' not in df.columns.to_list():
        indicator_id = context['indicator_id']
        series = helpers.sdmx.get_series_code_from_indicator_id(indicator_id)
        df['SERIES'] = series
    if 'UNIT_MEASURE' not in df.columns.to_list():
        df['UNIT_MEASURE'] = ''
    for index, row in df.iterrows():
        if row['UNIT_MEASURE'] == '':
            series = row['SERIES']
            unit = helpers.sdmx.get_unit_code_from_series_code(series)
            df.at[index, 'UNIT_MEASURE'] = unit
    return df


def alter_data(df, context):
    df = common_alterations(df)
    df = set_series_and_unit(df, context)
    df = set_time_detail(df)
    df = limit_to_national_ref_area(df, '508')
    return drop_columns(df)


def alter_indicator_id(indicator_id):
    return indicator_id.replace('.', '-')


def alter_meta(meta):
    for key in meta:
        if meta[key] is not None and isinstance(meta[key], str):
            meta[key] = meta[key].replace("'", "&#39;")
    return meta


config_path = 'config_data.yml'
open_sdg_build(config=config_path, alter_data=alter_data, alter_meta=alter_meta, alter_indicator_id=alter_indicator_id)
