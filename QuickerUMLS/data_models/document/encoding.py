from typing import Union


def mime_encoding(text: str) -> Union[str, None]:
    '''
    Use libmagic for encoding detection, pip install python-magic
    '''
    try:
        import magic
    except Exception as ex:
        return None
    else:
        magicobj = magic.Magic(mime_encoding=True)
        return magicobj.from_buffer(text)
