## How To Get API_KEY & ACCESS_TOKEN If Using Google SignIn Method On QtsApp Website.

1. Open The QuantsApp Website And login Using Google

2. After Login Inspect The Page Via Context Menu

3. Then Go To The Application Tab

4. Then Click on the Local Storage under the Storage Menu on the Left Sidebar

5. Then Click on the `https://web.quantsapp.com`

6. Then on the right hand side search for the `user` key.

7. Then click on the user key, which will provide you with the `API_KEY` as `userId` and `ACCESS_TOKEN` as `token`

8. Now Copy the `userId` Value and paste in the `.env` file under `API_KEY=`

8. Now Copy the `token` Value and paste in the `.env` file under `ACCESS_TOKEN=`

9. Now Save the `.env` file and close the browser and continue with the python program.

10. If You are finding difficulties in  following the above steps, you can watch the video tutorial at https://youtu.be/UQ2bM7ileRA
