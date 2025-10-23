import pandas as pd

def upsert(df: pd.DataFrame, *primary_keys: str, record: dict) -> pd.DataFrame:
    # 判断是否已有索引结构
    index_names = df.index.names if isinstance(df.index, pd.MultiIndex) else [df.index.name]
    has_index = all(k in index_names for k in primary_keys if k is not None)

    # 如果非空，尝试匹配并更新
    if not df.empty:
        condition = pd.Series(True, index=df.index)
        for key in primary_keys:
            if key in df.columns:
                condition &= (df[key] == record[key])
            elif has_index:
                condition &= (df.index.get_level_values(key) == record[key])
            else:
                raise KeyError(f"'{key}' not found in columns or index")

        if condition.any():
            for col, val in record.items():
                if col in df.columns:
                    df.loc[condition, col] = val
            return df

    # 插入新行
    index_vals = tuple(record[k] for k in primary_keys)
    data = {k: v for k, v in record.items() if not has_index or k not in primary_keys}

    new_row = pd.DataFrame([data], columns=df.columns if not df.columns.empty else None)

    if has_index:
        new_row.index = (
            pd.Index([index_vals[0]], name=primary_keys[0])
            if len(primary_keys) == 1
            else pd.MultiIndex.from_tuples([index_vals], names=primary_keys)
        )
        return pd.concat([df, new_row])
    else:
        return pd.concat([df, new_row], ignore_index=True)
