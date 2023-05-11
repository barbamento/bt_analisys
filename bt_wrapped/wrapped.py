import pandas as pd
import datetime
import os
import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud
import re
from key.key import API
from pyrogram import Client
from telethon.sync import TelegramClient
import utils as U
import ast
import json


def json_to_csv(path: str) -> None:
    """
    Returns a csv of the Telegram messages in the same folder as the json
    """
    df = pd.read_json(path)
    df = pd.DataFrame().from_records(df["messages"])
    df.to_csv(path.removesuffix(".json") + ".csv", index=False)


class wrapped:
    channel_json_path = "bt_wrapped/data/channel.json"
    channel_csv_path = channel_json_path.removesuffix(".json") + ".csv"
    group_json_path = "bt_wrapped/data/group.json"
    group_csv_path = group_json_path.removesuffix(".json") + ".csv"
    subscribers = 4630
    bt_group_id = -1001155308424

    def __init__(self) -> None:
        if not os.path.exists(self.channel_csv_path):
            json_to_csv(self.channel_json_path)
        if not os.path.exists(self.group_csv_path):
            json_to_csv(self.group_json_path)
        self.channel_df = pd.read_csv(self.channel_csv_path, index_col=False)
        self.group_df = pd.read_csv(self.group_csv_path, index_col=False)
        self.channel_df = self.channel_df.loc[
            (
                pd.to_datetime(self.channel_df["date"])
                >= datetime.datetime(2022, 1, 1, 0, 0, 0)
            )
            & (
                pd.to_datetime(self.channel_df["date"])
                < datetime.datetime(2023, 1, 1, 0, 0, 0)
            )
            & (self.channel_df["type"] == "message")
            & (~self.channel_df["id"].isin([20950, 20951]))
        ]
        new_replies = []
        self.group_df = self.group_df[
            (
                pd.to_datetime(self.group_df["date"])
                >= datetime.datetime(2022, 1, 1, 0, 0, 0)
            )
            & (
                (
                    pd.to_datetime(self.group_df["date"])
                    < datetime.datetime(2023, 1, 1, 0, 0, 0)
                )
                | (self.group_df["id"].isin(new_replies))
            )
            & (self.group_df["id"] != 534935)
        ]
        self.replies_only()
        if os.path.exists("temp/channel_df_2.csv"):
            self.channel_df = pd.read_csv("temp/channel_df.csv", index_col=False)
        else:
            self.channel_df_2 = self.replies_df.loc[
                self.replies_df["id"] == self.replies_df["root"]
            ]
            if os.path.exists("tmp.csv"):
                tmp = pd.read_csv("tmp.csv", index_col=False)
            else:
                with Client("pyro_session", API.app_id, API.api_hash) as client:
                    tmp = (
                        pd.DataFrame()
                        .from_records(
                            self.channel_df_2["id"].apply(
                                lambda x: U.get_engagement_from_chat_id(
                                    self.bt_group_id, x, client
                                )
                            )
                        )
                        .reset_index()
                    )
                    tmp.to_csv("tmp.csv", index=False)
            self.channel_df = pd.concat(
                [
                    self.channel_df.rename(
                        {"id": "id_in_channel"}, axis=1
                    ).reset_index(),
                    self.channel_df_2.rename(
                        {"id": "id_in_group"}, axis=1
                    ).reset_index(),
                    self.replies_df.groupby("root")
                    .size()
                    .reset_index()
                    .drop("root", axis=1),
                    tmp.reset_index(),
                ],
                axis=1,
            ).rename({0: "#_comments"}, axis=1)
            self.channel_df["total_reactions"] = (
                pd.DataFrame()
                .from_records(self.channel_df["reactions"].apply(ast.literal_eval))
                .sum(axis=1)
            )
            print(self.channel_df.columns)
            self.channel_df["engagement"] = (
                self.channel_df.loc[:, ["total_reactions", "#_comments"]].sum(axis=1)
                / self.subscribers
            )
            self.channel_df.to_csv("temp/channel_df.csv", index=False)
        print("job's done")

    def most_active_day(self) -> pd.Series:
        df = pd.DataFrame()
        df["date"] = self.group_df["date"].str.split("T").str[0]
        df = df.groupby("date").size()
        print(df.sort_values(ascending=False).head(50).sort_index())
        g = sns.lineplot(data=df)
        g.set_xticklabels(labels=[])
        plt.savefig("plots/yearly_recap.svg")
        return df.sort_values(ascending=False).head(50).sort_index()

    def most_active_time(self):
        df = self.group_df
        df["date"] = (
            df["date"].str.split("T").str[1].str.split(":").str[0]
            + ":"
            + df["date"].str.split("T").str[1].str.split(":").str[1]
        )
        df = df.groupby("date").size()
        g = sns.lineplot(data=df)
        print(df)
        plt.savefig("plots/time.pdf")

    def replies_only(self):
        """
        (self.group_df["from"] == "Best Timeline")
        & (self.group_df["forwarded_from"] == "Best Timeline")
        """
        roots = self.group_df[(self.group_df["saved_from"] == "Best Timeline")]
        roots["root"] = roots["id"]
        results = roots
        ids = roots
        j = 0
        while not ids.empty:
            tmp = self.group_df[self.group_df["reply_to_message_id"].isin(ids["id"])]
            right = results.loc[:, ["id", "root"]].rename({"id": "root_id"}, axis=1)
            tmp = tmp.merge(
                right, left_on="reply_to_message_id", right_on="root_id"
            ).drop("root_id", axis=1)
            ids = tmp
            results = pd.concat([results, tmp])
            j += 1
        self.replies_df: pd.DataFrame = results

    def emoticon(self):
        plt.close()
        tmp = (
            pd.DataFrame()
            .from_records(self.channel_df["reactions"].apply(ast.literal_eval))
            .sum(axis=0)
            .sort_values(ascending=False)
        )
        print(tmp)
        tmp.plot.pie(subplots=True, legend=False)
        plt.savefig("plots/emoticon_pie.svg")
        plt.savefig("plots/emoticon_pie.png")
        plt.close()

    def most_engaged_posts(self):
        print(
            [
                f"https://t.me/bestimeline/{i}"
                for i in self.channel_df.sort_values(
                    by="engagement", ascending=False
                ).head(10)["id_in_channel"]
            ]
        )

    def admins(self):
        admin_posts_raw = (
            self.channel_df.groupby("author").size().sort_values(ascending=False)
        )
        admin_posts_processed = pd.DataFrame(columns=["#_posts", "engagement"])
        for i in admin_posts_raw.index:
            if "cat" in i and "drunken" in i:
                j = "drunken cat"
            elif i in ["Pétta️️️️    ", "Pétta", "Pétta️️️️"]:
                j = "petta"
            elif "golden" in i:
                j = "golden doggo"
            elif "Gianni" in i:
                j = "Gianni Confuso"
            else:
                j = i
            if not j in admin_posts_processed.index:
                admin_posts_processed.loc[j, ["#_posts"]] = admin_posts_raw.loc[i]
            else:
                admin_posts_processed.loc[j, ["#_posts"]] += admin_posts_raw.loc[i]
        print(admin_posts_processed)
        plt.close()
        admin_posts_processed.loc[:, ["#_posts"]].plot.pie(subplots=True, legend=False)
        plt.savefig("plots/admins_pie.svg")
        plt.close()
        engagement_generated = self.channel_df.loc[:, ["author", "engagement"]]
        engagement_generated["engagement"] = (
            engagement_generated["engagement"] * self.subscribers
        )
        engagement_generated = engagement_generated.groupby("author")[
            "engagement"
        ].sum()
        # .set_index("author")
        admin_posts_processed = admin_posts_processed.fillna(0)
        for i in engagement_generated.index:
            print(i)
            if "cat" in i and "drunken" in i:
                j = "drunken cat"
            elif i in ["Pétta️️️️    ", "Pétta", "Pétta️️️️"]:
                j = "petta"
            elif "golden" in i:
                j = "golden doggo"
            elif "Gianni" in i:
                j = "Gianni Confuso"
            else:
                j = i

            admin_posts_processed.loc[j, ["engagement"]] += engagement_generated.loc[i]
        admin_posts_processed["engagement_per_post"] = (
            admin_posts_processed["engagement"] / admin_posts_processed["#_posts"]
        )
        print(admin_posts_processed)


if __name__ == "__main__":
    w = wrapped()
    w.admins()
    w.most_active_day()
    w.most_engaged_posts()
    # w.emoticon()
