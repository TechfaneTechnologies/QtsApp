# QtsApp
The Python Library For Qtsapp Which Displays The Option Chain In Near Real Time _([YT Video](https://youtu.be/XKFjufDaFhk))_. This Program Retrieves This Data From The Qtsapp Site And Then Generates Useful Analysis Of The Option Chain For The Specified Index Or Stock. It Also Continuously Refreshes The Option Chain Along With Implied Volatatlity (IV), Open Interest (OI), Delta, Theta, Vega, Gamma, Vanna, Charm, Speed, Zomma, Color, Volga, Veta At An Interval Of A Second And Visually Displays The Trend In Various Indicators Useful For Technical Analysis.

#### _If You have liked the library, Do Star This Repository and Stay-Up-To-Date_
<p align="center">
  <img src="https://user-images.githubusercontent.com/96371033/180197157-aabda812-828b-4cf7-97a6-a4b9bdd8b151.gif" alt="How To Star A Repository">
</p>

## Instructions on running the program for the first time

Install The QtsApp Library Via `pip install -U git+https://github.com/TechfaneTechnologies/QtsApp.git@dev` And Then Clone The Repo Via `git clone https://github.com/TechfaneTechnologies/QtsApp.git`

```shell
pip install -U git+https://github.com/TechfaneTechnologies/QtsApp.git@dev
git clone https://github.com/TechfaneTechnologies/QtsApp.git
```

_**If you are using UserId and Password Method To Login, Then Open the `.env.secret` and update the two fields `USER_NAME=Your_User_Name_or_Id` and `PASSWORD=Your_Password`, Now Save and Close the File.**_

_**Otherwise, If you are using google signin method, the above procedure will not work, and you have to get the `api_key` and `api_token` manually. And update the same in `.env` file instead of `.env.secret`, To know How To Do The Same, Please Follow This [Guideline](https://github.com/TechfaneTechnologies/QtsApp/blob/main/GetApiKeyAndAccessTokenFromBrowser.md)**_

**Post Updating And Saving The `.env` or `.env.secret` Then run the following commands to get the live data. _(Run During Market Times For Live Data)_**

```shell
cd QtsApp
python .\example.py
```

## Note
Keep the `.env`, `.env.secret` and `exaple.py` in the same directory before running `python example.py` to get the live data. _(Run During Market Times)_

## What's The Catch
With This  You can get live option chain data (of any NFO instrument and any valid expiry) along with iv, 1st, 2nd & 3rd Order greek from QtsApp, you just need to have a free account with them.

![QtsAppScreenshot](https://user-images.githubusercontent.com/68828793/178950834-dd3eb6e7-fbfd-40d4-a5c8-a49f87fa4a43.png)

https://www.youtube.com/watch?v=XKFjufDaFhk

https://www.youtube.com/watch?v=2TT9wkO2nH0

https://www.youtube.com/watch?v=7xujRROvIcY

https://www.youtube.com/watch?v=P26rGXSyNUk
