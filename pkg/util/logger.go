package util

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	//LevelError only
	LevelError = 1
	//LevelWarning and error
	LevelWarning = 2
	// LevelInfo LevelInfo,warning and error
	LevelInfo = 3
	// LevelTrace Info,warning,error and trace
	LevelTrace = 4
	// LevelVerbose All
	LevelVerbose = 5
)

var (
	suger       *zap.SugaredLogger
	atomicLevel zap.AtomicLevel
)

func InitLogger(logLevel int, isProduction bool, filename string) {
	atomicLevel = zap.NewAtomicLevelAt(toZapLevel(logLevel))

	//配置一个日志轮转
	lumberjackLogger := &lumberjack.Logger{
		Filename:   filename,
		MaxBackups: 3,
		MaxAge:     30,
		MaxSize:    100,
	}
	var core zapcore.Core
	if isProduction {
		core = zapcore.NewCore(
			zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
			zapcore.AddSync(lumberjackLogger),
			atomicLevel,
		)
	} else {
		core = zapcore.NewCore(
			zapcore.NewJSONEncoder(zap.NewDevelopmentEncoderConfig()),
			zapcore.AddSync(lumberjackLogger),
			atomicLevel,
		)
	}

	logger := zap.New(core)
	suger = logger.Sugar()
}

func toZapLevel(level int) zapcore.Level {
	switch level {
	case LevelError:
		return zap.ErrorLevel
	case LevelWarning:
		return zap.WarnLevel
	case LevelInfo:
		return zap.InfoLevel
	case LevelTrace, LevelVerbose:
		return zap.DebugLevel
	default:
		return zap.InfoLevel
	}
}

func setLogLevel(level int) {
	atomicLevel.SetLevel(toZapLevel(level))
}
func WriteLog(level int, format string, v ...interface{}) {
	switch level {
	case LevelError:
		suger.Errorf(format, v...)
	case LevelWarning:
		suger.Warnf(format, v...)
	case LevelInfo:
		suger.Infof(format, v...)
	case LevelTrace, LevelVerbose:
		suger.Debugf(format, v...)
	}
}

func WriteStructured(level int, msg string, fields map[string]interface{}) {
	zapFields := make([]zap.Field, 0, len(fields))
	for k, v := range fields {
		zapFields = append(zapFields, zap.Any(k, v))
	}

	switch level {
	case LevelError:
		suger.Errorw(msg, zapFields)
	case LevelWarning:
		suger.Warnw(msg, zapFields)
	case LevelInfo:
		suger.Infow(msg, zapFields)
	case LevelTrace, LevelVerbose:
		suger.Debugw(msg, zapFields)
	}
}

// WriteError writes an error log
func WriteError(format string, v ...interface{}) {
	WriteLog(LevelError, format, v...)
}

// WriteWarning writes a warning log
func WriteWarning(format string, v ...interface{}) {
	WriteLog(LevelWarning, format, v...)
}

// WriteInfo writes a information
func WriteInfo(format string, v ...interface{}) {
	WriteLog(LevelInfo, format, v...)
}

// WriteTrace writes traces and debug information
func WriteTrace(format string, v ...interface{}) {
	WriteLog(LevelTrace, format, v...)
}

// WriteVerbose writes verbose infromation
func WriteVerbose(format string, v ...interface{}) {
	WriteLog(LevelVerbose, format, v...)
}

// Panicf is equivalent to l.Printf() followed by a call to panic().
func Panicf(format string, v ...interface{}) {
	suger.Panicf(format, v...)
}

// Panicln is equivalent to l.Println() followed by a call to panic().
func Panicln(v ...interface{}) {
	suger.Panicln(v...)
}

// Fatalf is equivalent to Printf followed by os.Exit(1)
func Fatalf(format string, v ...interface{}) {
	suger.Fatalf(format, v...)
}
