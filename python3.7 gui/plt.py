import sys
from PyQt5 import QtGui
#from PyQt5.QtWidgets import QDialog, QApplication, QPushButton, QLabel
from PyQt5.QtWidgets import *
from mlxtend.plotting import plot_linear_regression
from sklearn.linear_model import LinearRegression

from matplotlib.backends.backend_qt4agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt4agg import NavigationToolbar2QT as NavigationToolbar
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
plt.style.use('seaborn-whitegrid')
import numpy
import random
import psycopg2
from scipy.stats import linregress

def get_report():
    conn = psycopg2.connect(host="127.0.0.1" , port="2223", database="postgres", user="postgres", password="postgres")
    cur = conn.cursor()
    statement = "SELECT * FROM reports"
    cur.execute(statement)
    rows = cur.fetchall()
    return rows
 #(76477, None, 2x'12100', '2015', '07', '06', None, '1', '1', '2', '9', '65', '7',
    #'21', '02', '0', '250', '0', '180', '0173', '0180', '3', '004', 23x'0', 24x'2', '', ''
    #, '7', '0', '3', '/', 31x'05', 32xNone, 33xNone, 34xNone, 35xNone)

class Window(QDialog):
    def __init__(self, parent=None):
        super(Window, self).__init__(parent)

        self.figure = Figure()
        self.canvas = FigureCanvas(self.figure)

        self.toolbar = NavigationToolbar(self.canvas, self)
        
        self.button = QPushButton('Plot')
        self.button.clicked.connect(self.plot)

        self.label1 = QLabel('Od dnia')
        self.lineEdit1 = QLineEdit("1")
        
        self.label5 = QLabel('Pearson cooeficient')
        self.lineEdit5 = QLineEdit()
        
        self.label3 = QLabel('Do dnia')
        self.lineEdit3 = QLineEdit("31")
        
        self.label4 = QLabel('WMOIND')
        self.lineEdit4 = QLineEdit("12100")
        
        #self.label3 = QLabel('Miejsce')
        #self.comboBox3 = QComboBox()
        
        layout = QVBoxLayout()
        layout.addWidget(self.toolbar)
        layout.addWidget(self.canvas)
        layout.addWidget(self.button)
        
        layout.addWidget(self.label1)
        layout.addWidget(self.lineEdit1)
        
        layout.addWidget(self.label3)
        layout.addWidget(self.lineEdit3)
        
        layout.addWidget(self.label4)
        layout.addWidget(self.lineEdit4)
        
        layout.addWidget(self.label5)
        layout.addWidget(self.lineEdit5)
        
        #layout.addWidget(self.label1)
        #layout.addWidget(self.comboBox3)
        
        self.setLayout(layout)

    def plot(self):
        rows = get_report()
        #data = [random.random() for i in range(10)]
        #ax = self.figure.add_subplot(111)
        #ax.clear()
        #ax.plot(data, '*-')
        xy = self.figure.add_subplot(111)
        #x = np.linspace(0, 10, 30)
        #y = np.sin(x)
        
        start_day = self.lineEdit1.text()
        #start_hour = self.lineEdit2.text()
        end_day = self.lineEdit3.text()
        wmoind = self.lineEdit4.text()
        
        x = []
        y = []
        for report in rows:
            if report[31] >= start_day and report[31] <= end_day:
                if wmoind == report[2] or wmoind == "*":
                    if report[23] is not None:
                        x.append(float(report[23]))
                        if report[33] is not None:
                            y.append(float(report[33]))
                        else:
                            y.append(0)
        
        xy.plot(x, y, 'o', color='black', label = "rain mm")
        pearson = numpy.corrcoef(x, y)[0, 1]
        self.lineEdit5.setText(str(pearson))
        
        slope, intercept, r_value, p_value, std_err = linregress(x,y)
        xl = []
        for each in x:
            xl.append(each*slope)
        line = xl + intercept

        xy.plot(x, line, color = "green", label = "linregress")
        #xy.plot(x, linreg.intercept + linreg.slope*(x), 'r', label='fitted line')
        #intercept, slope, corr_coeff = plot_linear_regression(x, y)
        #plt.show()
        xy.legend()
        xy.set_xlabel('synop mm', fontsize=18)
        xy.set_ylabel('radar mm', fontsize=16)
        self.canvas.draw()
        
        xy.clear()

if __name__ == '__main__':
    app = QApplication(sys.argv)

    main = Window()
    main.show()

    sys.exit(app.exec_())