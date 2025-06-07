# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=[],
    datas=[('C:\\Users\\smishra14\\setup\\miniconda\\envs\\fcst\\Lib\\site-packages\\pyecharts\\datasets', 'pyecharts\\datasets'),
     ('C:\\Users\\smishra14\\setup\\miniconda\\envs\\fcst\\Lib\\site-packages\\nicegui', 'nicegui'),
      ('C:\\Users\\smishra14\\setup\\miniconda\\envs\\fcst\\Lib\\site-packages\\setproctitle', 'setproctitle'),
      ('C:\\Users\\smishra14\\setup\\miniconda\\envs\\fcst\\Lib\\site-packages\\psutil', 'psutil'),
      ('C:\\Users\\smishra14\\setup\\miniconda\\envs\\fcst\\Lib\\site-packages\\ray', 'ray'),
      ('C:\\Users\\smishra14\\setup\\miniconda\\envs\\fcst\\Lib\\site-packages\\arrow_odbc', 'arrow_odbc')],
    hiddenimports=['setproctitle','psutil','ray','arrow_odbc','queue','multiprocessing.pool','concurrent.futures','_asyncio','threading',],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='main',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name='main',
)
