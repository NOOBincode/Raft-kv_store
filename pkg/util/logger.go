package util

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
)

const (
	// LevelError only
	LevelError = 1
	// LevelWarning and error
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

// InitLogger 初始化日志
func InitLogger(logLevel int, isProduction bool, filename string) {
	atomicLevel = zap.NewAtomicLevelAt(toZapLevel(logLevel))

	// 配置日志轮转
	lumberjackLogger := &lumberjack.Logger{
		Filename:   filename,
		MaxBackups: 3,   // 保留旧日志的最大数量
		MaxAge:     30,  // 保留旧日志的最大天数
		MaxSize:    100, // 每个日志文件的最大大小（MB）
	}

	// 创建多个 WriteSyncer
	fileWriter := zapcore.AddSync(lumberjackLogger)
	consoleWriter := zapcore.AddSync(os.Stderr)

	// 获取编码器
	encoder := getEncoder(isProduction)

	// 创建核心
	core := zapcore.NewTee(
		zapcore.NewCore(encoder, fileWriter, atomicLevel),
		zapcore.NewCore(encoder, consoleWriter, atomicLevel),
	)

	// 创建 Logger
	logger := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))
	suger = logger.Sugar()
}

// getEncoder 获取日志编码器
func getEncoder(isProduction bool) zapcore.Encoder {
	if isProduction {
		encoderConfig := zap.NewProductionEncoderConfig()
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		return zapcore.NewJSONEncoder(encoderConfig)
	}

	encoderConfig := zap.NewDevelopmentEncoderConfig()
	encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	return zapcore.NewConsoleEncoder(encoderConfig)
}

// toZapLevel 将自定义日志级别转换为 zapcore.Level
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

// SetLogLevel 动态设置日志级别
func SetLogLevel(level int) {
	atomicLevel.SetLevel(toZapLevel(level))
}

// WriteLog 写入日志
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

// WriteStructured 写入结构化日志
func WriteStructured(level int, msg string, fields map[string]interface{}) {
	kvs := make([]interface{}, 0, len(fields)*2)
	for k, v := range fields {
		kvs = append(kvs, k, v)
	}

	switch level {
	case LevelError:
		suger.Errorw(msg, kvs...)
	case LevelWarning:
		suger.Warnw(msg, kvs...)
	case LevelInfo:
		suger.Infow(msg, kvs...)
	case LevelTrace, LevelVerbose:
		suger.Debugw(msg, kvs...)
	}
}

// WriteError 写入错误日志
func WriteError(format string, v ...interface{}) {
	WriteLog(LevelError, format, v...)
}

// WriteWarning 写入警告日志
func WriteWarning(format string, v ...interface{}) {
	WriteLog(LevelWarning, format, v...)
}

// WriteInfo 写入信息日志
func WriteInfo(format string, v ...interface{}) {
	WriteLog(LevelInfo, format, v...)
}

// WriteTrace 写入调试日志
func WriteTrace(format string, v ...interface{}) {
	WriteLog(LevelTrace, format, v...)
}

// WriteVerbose 写入详细日志
func WriteVerbose(format string, v ...interface{}) {
	WriteLog(LevelVerbose, format, v...)
}

// Panicf 写入日志并触发 panic
func Panicf(format string, v ...interface{}) {
	suger.Panicf(format, v...)
}

// Panicln 写入日志并触发 panic
func Panicln(v ...interface{}) {
	suger.Panicln(v...)
}

// Fatalf 写入日志并退出程序
func Fatalf(format string, v ...interface{}) {
	suger.Fatalf(format, v...)
}
