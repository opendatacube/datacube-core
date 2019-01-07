#!/usr/bin/env python
"""
Interactive Pixel Drill for AGDCv2.

"""
# pylint: disable=import-error, wrong-import-position
# Unavoidable with TK class hierarchy.
# pylint: disable=too-many-ancestors, redefined-builtin
import argparse
import os
import sys
import warnings

import matplotlib
import numpy as np
import pandas as pd
import tkinter as tk

import datacube

matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import matplotlib.animation as anim
from matplotlib.backends.backend_tkagg import NavigationToolbar2TkAgg, ToolTip


# pylint: disable=invalid-name, too-many-locals, global-variable-undefined, too-many-statements, redefined-outer-name
# pylint: disable=broad-except

FONT = ("Helvetica", 9)

# Set our plot parameters

plt.rcParams.update({
    'legend.fontsize': 8,
    'legend.handlelength': 3,
    'axes.titlesize': 9,
    'axes.labelsize': 9,
    'xtick.labelsize': 9,
    'ytick.labelsize': 9,
    'font.family': 'sans'})


class Toolbar(NavigationToolbar2TkAgg):
    def __init__(self, canvas, parent):
        self.toolitems = (
            ('Unzoom', 'Reset original view', 'home', 'home'),
            ('Zoom', 'Zoom to rectangle', 'zoom_to_rect', 'zoom'),
            ('Pan', 'Pan axes with left mouse, zoom with right', 'move', 'pan'),
            (None, None, None, None),
            ('Save', 'Save', 'filesave', 'save_movie'),
            (None, None, None, None),
            ('Prev', 'Previous observation', 'back', 'backimg'),
            ('Next', 'Next observation', 'forward', 'fwdimg'),
            (None, None, None, None),
        )
        NavigationToolbar2TkAgg.__init__(self, canvas, parent)
        self._init_toolbar()
        self.configure(background='black')

    def _Button(self, text, file, command, extension='.gif'):
        b = tk.Button(master=self, text=text, padx=2, pady=2, command=command,
                      relief=tk.FLAT, font=FONT, justify=tk.CENTER)
        b.pack(side=tk.LEFT)
        return b

    def _init_toolbar(self):
        xmin, xmax = self.canvas.figure.bbox.intervalx
        height, width = 40, xmax - xmin
        tk.Frame.__init__(self, master=self.window,
                          width=int(width), height=int(height),
                          borderwidth=2)
        self.update()

        for text, tooltip_text, image_file, callback in self.toolitems:
            if text is None:
                pass
            else:
                button = self._Button(text=text, file=image_file,
                                      command=getattr(self, callback))
                if tooltip_text is not None:
                    ToolTip.createToolTip(button, tooltip_text)
                button.configure(background='black', foreground='white')

        self.message = tk.StringVar(master=self)
        self._label = tk.Label(master=self, textvariable=self.message,
                               font=FONT)
        self._label.pack(side=tk.RIGHT)
        self.pack(side=tk.BOTTOM, fill=tk.X)
        self._label.configure(background='black', foreground='white')

    def mouse_move(self, event):
        self._set_cursor(event)
        if event.inaxes and event.inaxes.get_navigate():
            try:
                s = event.inaxes.format_coord(event.xdata, event.ydata)
                self.set_message(s)
            except (ValueError, OverflowError):
                pass

    def fwdimg(self, *args):
        fwdimg()

    def backimg(self, *args):
        backimg()

    def save_movie(self, *args):
        filetypes = self.canvas.get_supported_filetypes().copy()
        default_filetype = self.canvas.get_default_filetype()

        default_filetype_name = filetypes[default_filetype]
        del filetypes[default_filetype]

        sorted_filetypes = list(filetypes.items())
        sorted_filetypes.sort()
        sorted_filetypes.insert(0, (default_filetype, default_filetype_name))

        defaultextension = ''
        initialdir = plt.rcParams.get('savefig.directory', '')
        initialdir = os.path.expanduser(initialdir)
        initialfile = 'movie.mp4'
        fname = tk.filedialog.asksaveasfilename(
            master=self.window,
            title='Save the stack',
            filetypes=[('MPEG 4', '*.mp4')],
            defaultextension=defaultextension,
            initialdir=initialdir,
            initialfile=initialfile,
        )

        if fname == "" or fname == ():
            return
        else:
            if initialdir == '':
                plt.rcParams['savefig.directory'] = initialdir
            else:
                plt.rcParams['savefig.directory'] = os.path.dirname(str(fname))
            try:
                writer = anim.writers['ffmpeg']
                mwriter = writer(fps=1,
                                 bitrate=0,
                                 codec='h264',
                                 # extra_args=['-crf', '23', '-pix_fmt' 'yuv420p'],
                                 metadata={})
                with mwriter.saving(mainfig, fname, 140):
                    print(' '.join(mwriter._args()))  # pylint: disable=protected-access
                    for i in range(ntime):
                        changeimg(i)
                        mwriter.grab_frame()
            except Exception as e:
                tk.messagebox.showerror("Error saving file", str(e))


class DrillToolbar(NavigationToolbar2TkAgg):
    def __init__(self, canvas, parent):
        self.toolitems = (
            ('CSV', 'Save CSV', 'filesave', 'save_csv'),
            ('FIG', 'Save figure', 'filesave', 'save_figure'),
        )
        NavigationToolbar2TkAgg.__init__(self, canvas, parent)
        self._init_toolbar()
        self.configure(background='black')

    def _Button(self, text, file, command, extension='.gif'):
        b = tk.Button(master=self, text=text, padx=2, pady=2, command=command,
                      relief=tk.FLAT, font=FONT)
        b.pack(side=tk.LEFT)
        return b

    def _init_toolbar(self):
        xmin, xmax = self.canvas.figure.bbox.intervalx
        height, width = 30, xmax - xmin
        tk.Frame.__init__(self, master=self.window,
                          width=int(width), height=int(height),
                          borderwidth=2)
        self.update()

        for text, tooltip_text, image_file, callback in self.toolitems:
            if text is None:
                # spacer, unhandled in Tk
                pass
            else:
                button = self._Button(text=text, file=image_file,
                                      command=getattr(self, callback))
                if tooltip_text is not None:
                    ToolTip.createToolTip(button, tooltip_text)
                button.configure(background='black', foreground='white')

        self.message = tk.StringVar(master=self)

    def save_csv(self, *args):
        initialdir = plt.rcParams.get('savefig.directory', '')
        initialdir = os.path.expanduser(initialdir)
        fname = tk.filedialog.asksaveasfilename(
            master=self.window,
            title='Save the pixel drill',
            filetypes=[('CSV', '*.csv')],
            defaultextension='',
            initialdir=initialdir,
            initialfile='pixeldrill.csv',
        )

        if fname == "" or fname == ():
            return
        else:
            if initialdir == '':
                plt.rcParams['savefig.directory'] = initialdir
            else:
                plt.rcParams['savefig.directory'] = os.path.dirname(str(fname))
            try:
                ds = pd.DataFrame(data=ts,
                                  index=times,
                                  columns=bands)
                ds.to_csv(fname)

            except Exception as e:
                tk.messagebox.showerror("Error saving file", str(e))

    def save_figure(self, *args):
        filetypes = self.canvas.get_supported_filetypes().copy()
        default_filetype = self.canvas.get_default_filetype()

        default_filetype_name = filetypes[default_filetype]
        del filetypes[default_filetype]

        sorted_filetypes = list(filetypes.items())
        sorted_filetypes.sort()
        sorted_filetypes.insert(0, (default_filetype, default_filetype_name))

        initialdir = plt.rcParams.get('savefig.directory', '')
        initialdir = os.path.expanduser(initialdir)
        initialfile = 'pixeldrill.pdf'
        fname = tk.filedialog.asksaveasfilename(
            master=self.window,
            title='Save the pixel drill',
            filetypes=[('PNG', '*.png'), ('PDF', '*.pdf')],
            defaultextension='',
            initialdir=initialdir,
            initialfile=initialfile,
        )

        if fname == "" or fname == ():
            return
        else:
            if initialdir == '':
                plt.rcParams['savefig.directory'] = initialdir
            else:
                plt.rcParams['savefig.directory'] = os.path.dirname(str(fname))
            try:
                fig = plt.figure(figsize=(6, 4.5))

                ax3 = fig.add_subplot(211, xmargin=0, ymargin=0)
                ax3.set_xticks(range(nband))
                ax3.set_xticklabels(bands)
                ax3.set_title('Spectral profiles through time')
                ax3.set_xlim((-0.2, nband - 0.8))
                ax3.set_ylim((0, np.nanmax(data)))
                ax3.xaxis.grid(color='black', linestyle='dotted')

                box = ax3.get_position()
                ax3.set_position([box.x0, box.y0 + box.height * 0.1,
                                  box.width, box.height * 0.8])

                tindex = range(1, len(times) + 1)

                ax4 = fig.add_subplot(212, xmargin=0, ymargin=0)
                ax4.set_title('Band time series')

                ax4.set_xticks(tindex)
                ax4.set_xlim(0.9, tindex[-1] + 0.1)
                ax4.set_ylim((0, np.nanmax(data)))

                for i, p in enumerate(ts.T):
                    ax3.plot(range(nband), p, c='k')

                for i in range(ts.shape[0]):
                    tt = ts[i, :]
                    ax4.plot(tindex, tt, lw=1,
                             marker='.', linestyle='-', color=colors[i],
                             label=bands[i])

                ax4.legend(loc='upper center', bbox_to_anchor=(0.5, -0.2),
                           labelspacing=0.8, handletextpad=0, handlelength=2,
                           borderaxespad=0, ncol=nband, columnspacing=0.5)

                fig.savefig(fname, bbox_inches='tight')

                # plt.close(fig)

            except Exception as e:
                tk.messagebox.showerror("Error saving file", str(e))


class Formatter(object):
    def __init__(self, vi, names, data):
        self.vi = vi
        self.names = names
        self.data = data

    def __call__(self, x, y):
        xi, yi = int(round(x, 0)), int(round(y, 0))
        values = ' '.join(['{}:{}'.format(n, d) for n, d in
                           zip(self.names, self.data[yi, xi, :, vi])])
        return 'x:{} y:{}\n{}'.format(xi, yi, values)


def dcmap(length, base_cmap=None):
    """Create an length-bin discrete colormap from the specified input map."""
    base = plt.cm.get_cmap(base_cmap)
    color_list = base(np.linspace(0, 1, length))
    cmap_name = base.name + str(length)
    return base.from_list(cmap_name, color_list, length)


def sizefmt(num, suffix='B'):
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def setfg(ax, color):
    """Set the color of the frame, major ticks, tick labels, axis labels,
    title and legend."""
    for tl in ax.get_xticklines() + ax.get_yticklines():
        tl.set_color(color)
    for spine in ax.spines:
        ax.spines[spine].set_edgecolor(color)
    for tick in ax.xaxis.get_major_ticks():
        tick.label1.set_color(color)
    for tick in ax.yaxis.get_major_ticks():
        tick.label1.set_color(color)
    ax.axes.xaxis.label.set_color(color)
    ax.axes.yaxis.label.set_color(color)
    ax.axes.xaxis.get_offset_text().set_color(color)
    ax.axes.yaxis.get_offset_text().set_color(color)
    ax.axes.title.set_color(color)
    lh = ax.get_legend()
    if lh is not None:
        lh.get_title().set_color(color)
        lh.legendPatch.set_edgecolor('none')
        labels = lh.get_texts()
        for lab in labels:
            lab.set_color(color)
    for tl in ax.get_xticklabels():
        tl.set_color(color)
    for tl in ax.get_yticklabels():
        tl.set_color(color)


def setbg(ax, color):
    """Set the background color of the current axes (and legend)."""
    ax.patch.set_facecolor(color)
    lh = ax.get_legend()
    if lh is not None:
        lh.legendPatch.set_facecolor(color)


def drill(x=0, y=0):
    """Do the pixel drill."""

    # Get slice

    global ts
    ts = data[y, x, :, :]

    # Plot spectral profile

    ax1.lines = []
    for i, p in enumerate(ts.T):
        ax1.plot(range(nband), p, c='w')
    # ax1.set_ylim((0, np.nanmax(ts)*1.2))

    # Plot time series

    ax2.lines = []
    for i in range(ts.shape[0]):
        tt = ts[i, :]
        ax2.plot(tindex, tt, lw=1,
                 marker='.', linestyle='-', color=colors[i],
                 label=bands[i])
    # ax2.set_xlim(-0.1, tindex[-1] + 0.1)
    # ax2.set_xticks(tindex)

    ax2.legend(loc='upper center', bbox_to_anchor=(0.5, -0.2),
               labelspacing=0.8, handletextpad=0, handlelength=2,
               borderaxespad=0, ncol=nband, columnspacing=0.5)

    setfg(ax2, 'white')
    setbg(ax2, 'black')

    # Update figure

    drillfig.canvas.set_window_title('Pixel drill @ ({},{})'.format(x, y))
    drillfig.canvas.draw()


def changeimg(i):
    """Change image shown."""
    global vi

    if vi == i:
        return

    # Scale and fix image
    img = data[:, :, vbnds, i].copy()
    mask = (img > maxvalue).any(axis=2)
    img = img / maxvalue
    img[mask] = 1.0
    mask = np.isnan(img).any(axis=2)
    img[mask] = 0.0

    # Draw it
    mainimg.set_data(img)
    mainfig.canvas.set_window_title('[{}/{}] {}. Data mem usage: {}'.format(i + 1, ntime, times[i], memusage))
    mainfig.canvas.draw()

    vi = i


def onclick(event):
    """Handle a click event on the main image."""
    global lastclick
    try:
        x = int(round(event.xdata))
        y = int(round(event.ydata))
        b = int(event.button)
        if b in [2, 3]:
            lastclick = (x, y)
            drill(x, y)
    except TypeError:
        pass


def onclickpd(event):
    """Handle a click event in the pixel drill."""
    global vi
    vi = int(round(event.xdata))
    changeimg(vi)


def onpress(event):
    """Handle a keyboard event."""

    if event.key == 'right':
        fwdimg()
        return

    if event.key == 'left':
        backimg()
        return


def fwdimg():
    """Show next observation."""
    i = min(vi + 1, data.shape[3] - 1)
    changeimg(i)


def backimg():
    """Show previous observation."""
    i = max(0, vi - 1)
    changeimg(i)


def run(latrange=None, lonrange=None, timerange=None, measurements=None,
        valuemax=None, product=None, groupby=None, verbose=False):
    """Do all the work."""

    # Keep track of some variables globally instead of wrapping
    # everything in a big object

    global vi
    global lastclick
    global data
    global ax, ax1, ax2
    global nband, tindex, colors, bands, vbnds, ntime, times
    global drillfig, mainfig, mainimg
    global maxvalue, memusage

    # Try to get data

    try:
        print('loading data from the datacube...', end='')

        # Query the data

        dc = datacube.Datacube()
        dcdata = dc.load(product=product,
                         measurements=measurements,
                         time=timerange,
                         latitude=latrange,
                         longitude=lonrange,
                         group_by=groupby)

        # Check that we have data returned

        if dcdata.data_vars == {}:
            print('loading data failed, no data in that range.')
            sys.exit(1)

        # Extract times and band information
        dcdata = dcdata.to_array(dim='band')

        times = dcdata.coords['time'].to_index().tolist()
        bands = dcdata.coords['band'].to_index().tolist()
        bcols = {b: i for i, b in enumerate(bands)}

        nband = len(bands)
        ntime = len(times)

        # Work out what to show for images

        visible = ['red', 'green', 'blue']
        if all([b in bands for b in visible]):
            vbnds = [bcols[b] for b in visible]
        elif len(bands) >= 3:
            vbnds = [bcols[b] for b in bands[:3]]
        else:
            vbnds = [0, 0, 0]

        print('done')

    except LookupError:
        print('failed')

        # Display a list of valid products

        if product is None:
            print('valid products are:')
            prods = dc.list_products()[['name', 'description']]
            print(prods.to_string(index=False,
                                  justify='left',
                                  header=False,
                                  formatters={'description': lambda s: '(' + s + ')'}))
        sys.exit(1)

    except Exception:
        print('failed')
        sys.exit(2)

    # Nasty but it has to be done

    data = dcdata.transpose('y', 'x', 'band', 'time').data.astype(np.float32)
    data[data == -999] = np.nan

    # Set variables

    vi = 0
    lastclick = (0, 0)
    memusage = sizefmt(data.nbytes)
    maxvalue = valuemax

    # Setup the main figure

    mainfig = plt.figure(figsize=(6, 6))
    mainfig.canvas.set_window_title('[{}/{}] {}. Data mem usage: {}'.format(1, ntime, times[0], memusage))
    mainfig.patch.set_facecolor('black')

    ax = plt.Axes(mainfig, [0., 0., 1., 1.])
    ax.format_coord = Formatter(vi, bands, data)
    ax.set_axis_off()
    ax.invert_yaxis()
    mainfig.add_axes(ax)

    # Surgery on the toolbar

    canvas = mainfig.canvas
    canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)

    window = mainfig.canvas.toolbar.window
    mainfig.canvas.toolbar.pack_forget()
    mainfig.canvas.toolbar = Toolbar(mainfig.canvas, window)
    mainfig.canvas.toolbar.update()
    mainfig.canvas.toolbar.pack(side=tk.BOTTOM, fill=tk.BOTH, expand=0)
    canvas.show()

    # Scale and fix visible image

    img = data[:, :, vbnds, 0].copy()
    mask = (img > maxvalue).any(axis=2)
    img = img / maxvalue
    img[mask] = 1.0
    mask = np.isnan(img).any(axis=2)
    img[mask] = 0.0

    # Show the image

    mainimg = plt.imshow(img, interpolation='nearest', origin='upper', aspect='auto', vmin=0, vmax=1)

    # Setup the drill figure

    drillfig = plt.figure(figsize=(4, 3))
    drillfig.patch.set_facecolor('black')
    drillfig.canvas.toolbar.pack_forget()

    # Surgery on the toolbar

    canvas = drillfig.canvas
    window = drillfig.canvas.toolbar.window
    drillfig.canvas.toolbar.pack_forget()
    drillfig.canvas.toolbar = DrillToolbar(drillfig.canvas, window)
    drillfig.canvas.toolbar.update()
    drillfig.canvas.toolbar.pack(side=tk.BOTTOM, fill=tk.BOTH, expand=0)
    canvas.show()

    # Spectral profile graph

    ax1 = drillfig.add_subplot(211, xmargin=0)
    ax1.set_xticks(range(nband))
    ax1.set_xticklabels(bands)
    ax1.set_title('Spectral profiles through time')
    ax1.set_xlim((-0.2, nband - 0.8))
    ax1.set_ylim((0, np.nanmax(data)))
    ax1.xaxis.grid(color='white', linestyle='dotted')

    setfg(ax1, 'white')
    setbg(ax1, 'black')

    box = ax1.get_position()
    ax1.set_position([box.x0, box.y0 + box.height * 0.2,
                      box.width, box.height * 0.8])

    # Time series graph

    tindex = range(1, len(times) + 1)

    ax2 = drillfig.add_subplot(212, xmargin=0)
    ax2.set_title('Band time series')

    ax2.set_xticks(tindex)
    # ax2.set_xticklabels(times)
    ax2.set_xlim(0.9, tindex[-1] + 0.1)
    ax2.set_ylim((0, np.nanmax(data)))

    setfg(ax2, 'white')
    setbg(ax2, 'black')

    box = ax2.get_position()
    ax2.set_position([box.x0, box.y0 + box.height * 0.2,
                      box.width, box.height * 0.8])

    # Work out colors for bands in time series

    colors = [m[0] for m in bands if m[0] in ['r', 'g', 'b']]
    ntoadd = max(0, len(bands) - len(colors))
    cmap = dcmap(ntoadd, 'spring')
    colors = colors + [cmap(i) for i in range(ntoadd)]

    drill(*lastclick)

    # Hook up the event handlers

    mainfig.canvas.mpl_connect('button_press_event', onclick)
    mainfig.canvas.mpl_connect('key_press_event', onpress)
    mainfig.canvas.mpl_connect('close_event', lambda x: plt.close())

    drillfig.canvas.mpl_connect('close_event', lambda x: plt.close())
    drillfig.canvas.mpl_connect('button_press_event', onclickpd)

    # Show it

    plt.show()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('-latrange',
                        help='latitude range',
                        nargs=2,
                        default=[-34.5, -35],
                        required=False)

    parser.add_argument('-lonrange',
                        help='longitude range',
                        nargs=2,
                        default=[148.5, 149],
                        required=False)

    parser.add_argument('-timerange',
                        help='time range',
                        nargs=2,
                        default=['2011-3-2', '2011-6-5'],
                        type=str,
                        required=False)

    parser.add_argument('-measurements',
                        help='measurement',
                        action='append',
                        type=str,
                        required=False)

    parser.add_argument('-product',
                        help='product',
                        required=False)

    parser.add_argument('-groupby',
                        help='groupby',
                        required=False)

    parser.add_argument('-valuemax',
                        help='max value',
                        type=float,
                        default=4000,
                        required=False)

    parser.add_argument('-verbose',
                        help='verbose output',
                        default=True,
                        required=False)

    args = parser.parse_args()
    kwargs = vars(args)

    if not args.product:
        parser.print_help()
        print('\n\nValid choices for PRODUCT are:')
        dc = datacube.Datacube()
        prods = dc.list_products()['name']
        print(prods.to_string(index=False, header=False))
        parser.exit()

    if args.verbose:
        print(kwargs)

    run(**kwargs)


if __name__ == '__main__':
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', r'All-NaN (slice|axis) encountered')
            main()
    except KeyboardInterrupt:
        pass
