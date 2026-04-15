package mw

import (
	"context"
	"fmt"
	"math"

	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/errinfo"
	"github.com/openimsdk/tools/checker"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"github.com/openimsdk/tools/mw/specialerror"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func RpcServerInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (_ any, err error) {
	method := info.FullMethod
	md, err := validateMetadata(ctx)
	if err != nil {
		return nil, err
	}
	ctx, err = enrichContextWithMetadata(ctx, md)
	if err != nil {
		return nil, err
	}
	defer func() {
		if r := recover(); r != nil {
			err = errs.ErrPanic(r)
			log.ZPanic(ctx, "rpc server panic", err, "method", method, "req", req)
		}
	}()
	//log.ZInfo(ctx, "rpc server request", "method", method, "req", req)
	if err := checker.Validate(req); err != nil {
		return nil, handleError(ctx, method, req, err)
	}
	resp, err := handler(ctx, req)
	if err != nil {
		return nil, handleError(ctx, method, req, err)
	}
	//log.ZInfo(ctx, "rpc server response success", "method", method, "req", req, "resp", resp)
	return resp, nil
}

func validateMetadata(ctx context.Context) (metadata.MD, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.New(codes.InvalidArgument, "missing metadata").Err()
	}
	if len(md.Get(constant.OperationID)) != 1 {
		return nil, status.New(codes.InvalidArgument, "operationID error").Err()
	}
	return md, nil
}

func enrichContextWithMetadata(ctx context.Context, md metadata.MD) (context.Context, error) {
	if keys := md.Get(constant.RpcCustomHeader); len(keys) > 0 {
		ctx = context.WithValue(ctx, constant.RpcCustomHeader, keys)
		for _, key := range keys {
			values := md.Get(key)
			if len(values) == 0 {
				return nil, status.New(codes.InvalidArgument, fmt.Sprintf("missing metadata key %s", key)).Err()
			}
			ctx = context.WithValue(ctx, key, values)
		}
	}
	ctx = context.WithValue(ctx, constant.OperationID, md.Get(constant.OperationID)[0])
	if opts := md.Get(constant.OpUserID); len(opts) == 1 {
		ctx = context.WithValue(ctx, constant.OpUserID, opts[0])
	}
	if opts := md.Get(constant.OpUserPlatform); len(opts) == 1 {
		ctx = context.WithValue(ctx, constant.OpUserPlatform, opts[0])
	}
	if opts := md.Get(constant.ConnID); len(opts) == 1 {
		ctx = context.WithValue(ctx, constant.ConnID, opts[0])
	}
	return ctx, nil
}

func handleError(ctx context.Context, method string, req any, err error) error {
	codeErr := getErrData(err)
	log.ZAdaptive(ctx, "rpc server response failed", err, "method", method, "req", req)

	// 将自定义错误码映射到标准gRPC状态码
	var grpcCode codes.Code
	switch codeErr.Code() {
	case 20012: // Forbidden
		grpcCode = codes.PermissionDenied
	case 20001: // PasswordError
		grpcCode = codes.Unauthenticated
	case 20002: // AccountNotFound
		grpcCode = codes.NotFound
	case 20003, 20004, 20014: // PhoneAlreadyRegister, AccountAlreadyRegister, EmailAlreadyRegister
		grpcCode = codes.AlreadyExists
	case 20005: // VerifyCodeSendFrequently
		grpcCode = codes.ResourceExhausted
	case 20006, 20007, 20008, 20009: // VerifyCode related
		grpcCode = codes.InvalidArgument
	case 20010, 20011: // InvitationCode related
		grpcCode = codes.FailedPrecondition
	case 20013: // RefuseFriend
		grpcCode = codes.PermissionDenied
	default:
		// 对于其他错误码，使用原来的逻辑，但确保在有效范围内
		if codeErr.Code() > 0 && int64(codeErr.Code()) <= int64(math.MaxUint32) {
			grpcCode = codes.Code(codeErr.Code())
		} else {
			grpcCode = codes.Internal
		}
	}

	grpcStatus := status.New(grpcCode, codeErr.Msg())
	errInfo := &errinfo.ErrorInfo{Cause: codeErr.Detail()}
	details, err := grpcStatus.WithDetails(errInfo)
	if err != nil {
		log.ZError(ctx, "rpc server response WithDetails failed", err, "method", method, "req", req)
		return errs.WrapMsg(err, "rpc server resp WithDetails error", "err", err)
	}
	return details.Err()
}

func getErrData(err error) errs.CodeError {
	var (
		code        int
		msg, detail string
	)
	codeErr := specialerror.ErrCode(err)
	if codeErr != nil {
		code = codeErr.Code()
		msg = codeErr.Msg()
		detail = codeErr.Detail()
	} else {
		code = errs.ServerInternalError
	}
	if code <= 0 || int64(code) > int64(math.MaxUint32) {
		code = errs.ServerInternalError
	}

	if msg == "" || detail == "" {
		stringErr := specialerror.ErrString(err)
		wrapErr := specialerror.ErrWrapper(err)

		if stringErr != nil {
			if msg == "" {
				msg = stringErr.Error()
			}
		}

		if wrapErr != nil {
			if msg == "" {
				msg = wrapErr.Error()
			}
			if detail == "" {
				detail = wrapErr.Error()
			}
		}
	}
	if msg == "" {
		msg = err.Error()
	}
	if detail == "" {
		detail = msg
	}

	return errs.NewCodeError(code, msg).WithDetail(detail)
}

func GrpcServer() grpc.ServerOption {
	return grpc.ChainUnaryInterceptor(RpcServerInterceptor)
}
