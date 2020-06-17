package org.apache.sysds.hops.codegen.cplan.cpp;

import org.apache.commons.lang.StringUtils;
import org.apache.sysds.hops.codegen.cplan.CNodeBinary;
import org.apache.sysds.hops.codegen.cplan.CNodeTernary;
import org.apache.sysds.hops.codegen.cplan.CNodeUnary;
import org.apache.sysds.hops.codegen.cplan.CodeTemplate;
import org.apache.sysds.runtime.codegen.SpoofCellwise;

import static org.apache.sysds.runtime.matrix.data.LibMatrixNative.isSinglePrecision;

public class Unary implements CodeTemplate {
    @Override
    public String getTemplate(CNodeUnary.UnaryType type, boolean sparse) {
        if(isSinglePrecision()) {
            switch( type ) {
                case ROW_SUMS:
                case ROW_SUMSQS:
                case ROW_MINS:
                case ROW_MAXS:
                case ROW_MEANS:
                case ROW_COUNTNNZS: {
                    String vectName = StringUtils.capitalize(type.name().substring(4, type.name().length()-1).toLowerCase());
                    return sparse ? "    T %TMP% = LibSpoofPrimitives.vect"+vectName+"(%IN1v%, %IN1i%, %POS1%, alen, len);\n":
                        "    T %TMP% = LibSpoofPrimitives.vect"+vectName+"(%IN1%, %POS1%, %LEN%);\n";
                }

                case VECT_EXP:
                case VECT_POW2:
                case VECT_MULT2:
                case VECT_SQRT:
                case VECT_LOG:
                case VECT_ABS:
                case VECT_ROUND:
                case VECT_CEIL:
                case VECT_FLOOR:
                case VECT_SIGN:
                case VECT_SIN:
                case VECT_COS:
                case VECT_TAN:
                case VECT_ASIN:
                case VECT_ACOS:
                case VECT_ATAN:
                case VECT_SINH:
                case VECT_COSH:
                case VECT_TANH:
                case VECT_CUMSUM:
                case VECT_CUMMIN:
                case VECT_CUMMAX:
                case VECT_SPROP:
                case VECT_SIGMOID: {
                    String vectName = type.getVectorPrimitiveName();
                    return sparse ? "    T[] %TMP% = LibSpoofPrimitives.vect"+vectName+"Write(%IN1v%, %IN1i%, %POS1%, alen, len);\n" :
                        "    T[] %TMP% = LibSpoofPrimitives.vect"+vectName+"Write(%IN1%, %POS1%, %LEN%);\n";
                }

                case EXP:
                    return "    T %TMP% = expf(%IN1%);\n";
                case LOOKUP_R:
                    return sparse ?
                        "    T %TMP% = getValue(%IN1v%, %IN1i%, ai, alen, 0);\n" :
                        "    T %TMP% = getValue(%IN1%, rix);\n";
                case LOOKUP_C:
                    return "    T %TMP% = getValue(%IN1%, n, 0, cix);\n";
                case LOOKUP_RC:
                    return "    T %TMP% = getValue(%IN1%, n, rix, cix);\n";
                case LOOKUP0:
                    return "    T %TMP% = %IN1%[0];\n";
                case POW2:
                    return "    T %TMP% = %IN1% * %IN1%;\n";
                case MULT2:
                    return "    T %TMP% = %IN1% + %IN1%;\n";
                case ABS:
                    return "    T %TMP% = fabsf(%IN1%);\n";
                case SIN:
                    return "    T %TMP% = sinf(%IN1%);\n";
                case COS:
                    return "    T %TMP% = cosf(%IN1%);\n";
                case TAN:
                    return "    T %TMP% = tanf(%IN1%);\n";
                case ASIN:
                    return "    T %TMP% = asinf(%IN1%);\n";
                case ACOS:
                    return "    T %TMP% = acosf(%IN1%);\n";
                case ATAN:
                    return "    T %TMP% = atanf(%IN1%);\n";
                case SINH:
                    return "    T %TMP% = sinhf(%IN1%);\n";
                case COSH:
                    return "    T %TMP% = coshf(%IN1%);\n";
                case TANH:
                    return "    T %TMP% = tanhf(%IN1%);\n";
                case SIGN:
                    return "    T %TMP% = signbit(%IN1%) == 0 ? 1.0f : -1.0f;\n";
                case SQRT:
                    return "    T %TMP% = sqrtf(%IN1%);\n";
                case LOG:
                    return "    T %TMP% = logf(%IN1%);\n";
                case ROUND:
                    return "    T %TMP% = roundf(%IN1%);\n";
                case CEIL:
                    return "    T %TMP% = ceilf(%IN1%);\n";
                case FLOOR:
                    return "    T %TMP% = floorf(%IN1%);\n";
                case SPROP:
                    return "    T %TMP% = %IN1% * (1 - %IN1%);\n";
                case SIGMOID:
                    return "    T %TMP% = 1 / (1 + expf(-%IN1%));\n";
                case LOG_NZ:
                    return "    T %TMP% = (%IN1%==0) ? 0 : logf(%IN1%);\n";

                default:
                    throw new RuntimeException("Invalid unary type: "+this.toString());
            }
        }
        else { /* double precision */
            switch( type ) {
                case ROW_SUMS:
                case ROW_SUMSQS:
                case ROW_MINS:
                case ROW_MAXS:
                case ROW_MEANS:
                case ROW_COUNTNNZS: {
                    String vectName = StringUtils.capitalize(type.name().substring(4, type.name().length()-1).toLowerCase());
                    return sparse ? "    T %TMP% = LibSpoofPrimitives.vect"+vectName+"(%IN1v%, %IN1i%, %POS1%, alen, len);\n":
                        "    T %TMP% = LibSpoofPrimitives.vect"+vectName+"(%IN1%, %POS1%, %LEN%);\n";
                }

                case VECT_EXP:
                case VECT_POW2:
                case VECT_MULT2:
                case VECT_SQRT:
                case VECT_LOG:
                case VECT_ABS:
                case VECT_ROUND:
                case VECT_CEIL:
                case VECT_FLOOR:
                case VECT_SIGN:
                case VECT_SIN:
                case VECT_COS:
                case VECT_TAN:
                case VECT_ASIN:
                case VECT_ACOS:
                case VECT_ATAN:
                case VECT_SINH:
                case VECT_COSH:
                case VECT_TANH:
                case VECT_CUMSUM:
                case VECT_CUMMIN:
                case VECT_CUMMAX:
                case VECT_SPROP:
                case VECT_SIGMOID: {
                    String vectName = type.getVectorPrimitiveName();
                    return sparse ? "    T[] %TMP% = LibSpoofPrimitives.vect"+vectName+"Write(%IN1v%, %IN1i%, %POS1%, alen, len);\n" :
                        "    T[] %TMP% = LibSpoofPrimitives.vect"+vectName+"Write(%IN1%, %POS1%, %LEN%);\n";
                }

                case EXP:
                    return "    T %TMP% = exp(%IN1%);\n";
                case LOOKUP_R:
                    return sparse ?
                        "    T %TMP% = getValue(%IN1v%, %IN1i%, ai, alen, 0);\n" :
                        "    T %TMP% = getValue(%IN1%, rix);\n";
                case LOOKUP_C:
                    return "    T %TMP% = getValue(%IN1%, n, 0, cix);\n";
                case LOOKUP_RC:
                    return "    T %TMP% = getValue(%IN1%, n, rix, cix);\n";
                case LOOKUP0:
                    return "    T %TMP% = %IN1%[0];\n";
                case POW2:
                    return "    T %TMP% = %IN1% * %IN1%;\n";
                case MULT2:
                    return "    T %TMP% = %IN1% + %IN1%;\n";
                case ABS:
                    return "    T %TMP% = fabs(%IN1%);\n";
                case SIN:
                    return "    T %TMP% = sin(%IN1%);\n";
                case COS:
                    return "    T %TMP% = cos(%IN1%);\n";
                case TAN:
                    return "    T %TMP% = tan(%IN1%);\n";
                case ASIN:
                    return "    T %TMP% = asin(%IN1%);\n";
                case ACOS:
                    return "    T %TMP% = acos(%IN1%);\n";
                case ATAN:
                    return "    T %TMP% = atan(%IN1%);\n";
                case SINH:
                    return "    T %TMP% = sinh(%IN1%);\n";
                case COSH:
                    return "    T %TMP% = cosh(%IN1%);\n";
                case TANH:
                    return "    T %TMP% = tanh(%IN1%);\n";
                case SIGN:
                    return "    T %TMP% = signbit(%IN1%) == 0 ? 1.0f : -1.0f;\n";
                case SQRT:
                    return "    T %TMP% = sqrt(%IN1%);\n";
                case LOG:
                    return "    T %TMP% = log(%IN1%);\n";
                case ROUND:
                    return "    T %TMP% = round(%IN1%);\n";
                case CEIL:
                    return "    T %TMP% = ceil(%IN1%);\n";
                case FLOOR:
                    return "    T %TMP% = floor(%IN1%);\n";
                case SPROP:
                    return "    T %TMP% = %IN1% * (1 - %IN1%);\n";
                case SIGMOID:
                    return "    T %TMP% = 1 / (1 + exp(-%IN1%));\n";
                case LOG_NZ:
                    return "    T %TMP% = (%IN1%==0) ? 0 : log(%IN1%);\n";

                default:
                    throw new RuntimeException("Invalid unary type: "+this.toString());
            }

        }
    }

    @Override
    public String getTemplate() {
        throw new RuntimeException("Calling wrong getTemplate method on " + getClass().getCanonicalName());
    }

    @Override
    public String getTemplate(SpoofCellwise.CellType ct) {
        throw new RuntimeException("Calling wrong getTemplate method on " + getClass().getCanonicalName());
    }

    @Override
    public String getTemplate(CNodeBinary.BinType type, boolean sparseLhs, boolean sparseRhs, boolean scalarVector, boolean scalarInput) {
        throw new RuntimeException("Calling wrong getTemplate method on " + getClass().getCanonicalName());
    }

    @Override
    public String getTemplate(CNodeTernary.TernaryType type, boolean sparse) {
        throw new RuntimeException("Calling wrong getTemplate method on " + getClass().getCanonicalName());
    }
}
