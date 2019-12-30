class PhasingTimeoutError(Exception):
    def __init__(self, msg, window_chrom, window_start, window_end):
        super().__init__(msg)
        self.window_chrom = window_chrom
        self.window_start = window_start
        self.window_end   = window_end

