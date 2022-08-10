# -*- coding: utf-8 -*-

"""
Created on Mon August 8, 08:09:56 2022
@author: DrJuneMoone
"""

from qtsapp.lib import *
from qtsapp.QTSAppUser import QTSAppUser
from qtsapp.QTSAppStream import QTSAppStream

testing = False


def process_with_shared_memory(lock, q, _wb_name: str):
    print(f"With SharedMemory: {current_process()=}")
    _np = None
    while True:
        if _np is not None:
            try:
                update_val(
                    _wb_name=_wb_name,
                    _ws_range="O307",
                    _value=_np,
                    convert=np.array,
                    expand="table",
                )
            except:
                update_val(
                    _wb_name=_wb_name,
                    _ws_range="O307",
                    _value=_np,
                    convert=np.array,
                    expand="table",
                )
                continue
        try:
            _mode, _shm_name, _shape, _dtype = q.get_nowait()
            with lock:
                print(_mode)
                # print(_mode, _shm_name, _shape, _dtype)
        except queue.Empty:
            _mode, _shm_name, _shape, _dtype = None, None, None, None
            continue
        try:
            if _mode == "shm_open" and _shm_name and _shape and _dtype:
                with lock:
                    _shm = SharedMemory(_shm_name)
                    _np = np.recarray(shape=_shape, dtype=_dtype, buf=_shm.buf)
                    continue
            if _mode == "shm_close":
                with lock:
                    print("Closing SharedMemory")
                    _shm.close()
                    _shm.unlink()
                    del _shm
                    del _np
                    _np = None
                q.put_nowait("Closed_SharedMemory")
                sleep(2)
                with lock:
                    print("Sent Command Closed_SharedMemory")
                continue
        except:
            continue


def is_running_live(_stream):
    return True if (isMarketTime(testing) and _stream) else False


def QtsAppRun(_stream=False, **kwargs):
    qtsapp = (
        QTSAppStream(**kwargs)
        if (isMarketTime(testing) and _stream)
        else QTSAppUser(**kwargs)
    )

    _is_running_live = is_running_live(_stream)

    def on_close(ws, code, reason):
        ws.stop()

    def on_error(ws, code, reason):
        raise Exception(code, reason)

    qtsapp.on_close = on_close
    qtsapp.on_error = on_error
    qtsapp.connect()
    sleep(10)
    return _is_running_live, qtsapp


def EnchantQtsApp(
    no_of_process: int = 1,
    stream: bool = False,
    _is_running_live: bool = False,
    first_connect: bool = True,
    wb_name: str = "OptionChain",
):
    lock = mLock()
    _processes = []
    q = mqueue()
    while True:
        try:
            if first_connect:
                _is_running_live, qtsapp = QtsAppRun(
                    _stream=is_running_live(_stream=stream),
                    wb_name=wb_name,
                    out_queue=q,
                )
                first_connect = False
                _processes = [
                    Process(
                        target=process_with_shared_memory,
                        args=(lock, q, wb_name),
                        name=f"Excel Worker Process No.{i}",
                        daemon=True,
                    ).start()
                    for i in range(no_of_process)
                ]

            elif _is_running_live != is_running_live(_stream=stream):
                del qtsapp
                if len(_processes) >= 1:
                    for _process in _processes:
                        _process.join()
                        _process.stop()
                _is_running_live, qtsapp = QtsAppRun(
                    _stream=is_running_live(_stream=stream),
                    wb_name=wb_name,
                    out_queue=q,
                )
                _processes = [
                    Process(
                        target=process_with_shared_memory,
                        args=(lock, q, wb_name),
                        name=f"Excel Worker Process No.{i}",
                        daemon=True,
                    ).start()
                    for i in range(no_of_process)
                ]
            else:
                sleep(60)
        except KeyboardInterrupt:
            print("Keyboard interrupt caught")
            for _process in _processes:
                _process.join()
                _process.stop()
            break
        except:
            print("caught a WS exception - going to restart in a few seconds")
            del qtsapp
            if len(_processes) >= 1:
                for _process in _processes:
                    _process.join()
                    _process.stop()
            _is_running_live, qtsapp = QtsAppRun(
                _stream=is_running_live(_stream=stream),
                wb_name=wb_name,
                out_queue=q,
            )
            _processes = [
                Process(
                    target=process_with_shared_memory,
                    args=(lock, q, wb_name),
                    name=f"Excel Worker Process No.{i}",
                    daemon=True,
                ).start()
                for i in range(no_of_process)
            ]
            continue
        finally:
            pass
