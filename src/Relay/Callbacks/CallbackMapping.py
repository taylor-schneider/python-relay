class CallbackMapping:
    def __init__(self, callback, args, kwargs):
        self.callback = callback
        self.args = args
        self.kwargs = kwargs