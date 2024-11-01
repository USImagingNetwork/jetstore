package compute_pipes

import (
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"strings"
)

func (ctx *BuilderContext) StartFanOutPipe(spec *PipeSpec, source *InputChannel, writePartitionsResultCh chan ComputePipesResult) {
	var cpErr, err error
	evaluators := make([]PipeTransformationEvaluator, len(spec.Apply))

	defer func() {
		// Catch the panic that might be generated downstream
		if r := recover(); r != nil {
			var buf strings.Builder
			buf.WriteString(fmt.Sprintf("StartFanOutPipe: recovered error: %v\n", r))
			buf.WriteString(string(debug.Stack()))
			cpErr := errors.New(buf.String())
			log.Println(cpErr)
			ctx.errCh <- cpErr
			// Avoid closing a closed channel
			select {
			case <-ctx.done:
			default:
				close(ctx.done)
			}
		}
		// Closing the output channels
		fmt.Println("**!@@ FanOutPipe: Closing Output Channels")
		oc := make(map[string]bool)
		for i := range spec.Apply {
			oc[spec.Apply[i].OutputChannel.Name] = true
		}
		for i := range oc {
			fmt.Println("**!@@ FanOutPipe: Closing Output Channel",i)
			ctx.channelRegistry.CloseChannel(i)
		}
		close(writePartitionsResultCh)
	}()

	for j := range spec.Apply {
		eval, err := ctx.buildPipeTransformationEvaluator(source, nil, writePartitionsResultCh, &spec.Apply[j])
		if err != nil {
			cpErr = fmt.Errorf("while calling buildPipeTransformationEvaluator for %s: %v", spec.Apply[j].Type, err)
			goto gotError
		}
		evaluators[j] = eval
	}

	// fmt.Println("**!@@ start fan_out loop on source:", source.config.Name)
	for inRow := range source.channel {
		for i := range spec.Apply {
			err = evaluators[i].apply(&inRow)
			if err != nil {
				cpErr = fmt.Errorf("while calling apply on PipeTransformationEvaluator (in fan_out): %v", err)
				goto gotError
			}
		}
	}
	// fmt.Println("Closing fan_out PipeTransformationEvaluator")
	for i := range evaluators {
		if evaluators[i] != nil {
			err = evaluators[i].done()
			if err != nil {
				cpErr = fmt.Errorf("while calling done on PipeTransformationEvaluator (in fan_out): %v", err)
				log.Println(cpErr)
				goto gotError
			}
			evaluators[i].finally()
		}
	}

	// All good!
	return

gotError:
	for i := range evaluators {
		if evaluators[i] != nil {
			evaluators[i].finally()
		}
	}
	log.Println(cpErr)
	ctx.errCh <- cpErr
	// Avoid closing a closed channel
	select {
	case <-ctx.done:
	default:
		close(ctx.done)
	}
}
