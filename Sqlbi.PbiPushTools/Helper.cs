using TabModel = Microsoft.AnalysisServices.Tabular;

namespace Sqlbi.PbiPushTools
{
    public static class Helper
    {
        public static string CardinalityText(this TabModel.SingleColumnRelationship sr)
        {
            return $" {((sr.FromCardinality == TabModel.RelationshipEndCardinality.Many) ? '*' : '1')}--{ ((sr.ToCardinality == TabModel.RelationshipEndCardinality.Many) ? '*' : '1')} ";
        }
    }
}